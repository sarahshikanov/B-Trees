package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A leaf of a B+ tree. Every leaf in a B+ tree of order d stores between d and
 * 2d (key, record id) pairs and a pointer to its right sibling (i.e. the page
 * number of its right sibling). Moreover, every leaf node is serialized and
 * persisted on a single page; see toBytes and fromBytes for details on how a
 * leaf is serialized. For example, here is an illustration of two order 2
 * leafs connected together:
 *
 *   leaf 1 (stored on some page)          leaf 2 (stored on some other page)
 *   +-------+-------+-------+-------+     +-------+-------+-------+-------+
 *   | k0:r0 | k1:r1 | k2:r2 |       | --> | k3:r3 | k4:r4 |       |       |
 *   +-------+-------+-------+-------+     +-------+-------+-------+-------+
 */
class LeafNode extends BPlusNode {
    // Metadata about the B+ tree that this node belongs to.
    private BPlusTreeMetadata metadata;

    // Buffer manager
    private BufferManager bufferManager;

    // Lock context of the B+ tree
    private LockContext treeContext;

    // The page on which this leaf is serialized.
    private Page page;

    // The keys and record ids of this leaf. `keys` is always sorted in ascending
    // order. The record id at index i corresponds to the key at index i. For
    // example, the keys [a, b, c] and the rids [1, 2, 3] represent the pairing
    // [a:1, b:2, c:3].
    //
    // Note the following subtlety. keys and rids are in-memory caches of the
    // keys and record ids stored on disk. Thus, consider what happens when you
    // create two LeafNode objects that point to the same page:
    //
    //   BPlusTreeMetadata meta = ...;
    //   int pageNum = ...;
    //   LockContext treeContext = new DummyLockContext();
    //
    //   LeafNode leaf0 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
    //   LeafNode leaf1 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
    //
    // This scenario looks like this:
    //
    //   HEAP                        | DISK
    //   ===============================================================
    //   leaf0                       | page 42
    //   +-------------------------+ | +-------+-------+-------+-------+
    //   | keys = [k0, k1, k2]     | | | k0:r0 | k1:r1 | k2:r2 |       |
    //   | rids = [r0, r1, r2]     | | +-------+-------+-------+-------+
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //   leaf1                       |
    //   +-------------------------+ |
    //   | keys = [k0, k1, k2]     | |
    //   | rids = [r0, r1, r2]     | |
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //
    // Now imagine we perform on operation on leaf0 like leaf0.put(k3, r3). The
    // in-memory values of leaf0 will be updated and they will be synced to disk.
    // But, the in-memory values of leaf1 will not be updated. That will look
    // like this:
    //
    //   HEAP                        | DISK
    //   ===============================================================
    //   leaf0                       | page 42
    //   +-------------------------+ | +-------+-------+-------+-------+
    //   | keys = [k0, k1, k2, k3] | | | k0:r0 | k1:r1 | k2:r2 | k3:r3 |
    //   | rids = [r0, r1, r2, r3] | | +-------+-------+-------+-------+
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //   leaf1                       |
    //   +-------------------------+ |
    //   | keys = [k0, k1, k2]     | |
    //   | rids = [r0, r1, r2]     | |
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //
    // Make sure your code (or your tests) doesn't use stale in-memory cached
    // values of keys and rids.
    private List<DataBox> keys;
    private List<RecordId> rids;

    // If this leaf is the rightmost leaf, then rightSibling is Optional.empty().
    // Otherwise, rightSibling is Optional.of(n) where n is the page number of
    // this leaf's right sibling.
    private Optional<Long> rightSibling;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * Construct a brand new leaf node. This constructor will fetch a new pinned
     * page from the provided BufferManager `bufferManager` and persist the node
     * to that page.
     */
    LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
             List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
             keys, rids,
             rightSibling, treeContext);
    }

    /**
     * Construct a leaf node that is persisted to page `page`.
     */
    private LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                     List<DataBox> keys,
                     List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        try {
            assert (keys.size() == rids.size());
            assert (keys.size() <= 2 * metadata.getOrder());

            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.rids = new ArrayList<>(rids);
            this.rightSibling = rightSibling;

            sync();
        } finally {
            page.unpin();
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    // See BPlusNode.get.
    @Override
    public LeafNode get(DataBox key) {
        // TODO(proj2): implement

        //Below is description of what this get function does:
        /**
         * n.get(k) returns the leaf node on which k may reside when queried from n.
         * For example, consider the following B+ tree (for brevity, only keys are
         * shown; record ids are omitted).
         *
         *                               inner
         *                               +----+----+----+----+
         *                               | 10 | 20 |    |    |
         *                               +----+----+----+----+
         *                              /     |     \
         *                         ____/      |      \____
         *                        /           |           \
         *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
         *   |  1 |  2 |  3 |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
         *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
         *   leaf0                  leaf1                  leaf2
         *
         * inner.get(x) should return
         *
         *   - leaf0 when x < 10,
         *   - leaf1 when 10 <= x < 20, and
         *   - leaf2 when x >= 20.
         *
         * Note that inner.get(4) would return leaf0 even though leaf0 doesn't
         * actually contain 4.
         */

        return this; //Filler value, come back to this, might be correct since "base case"
        //Above could actually be right since this is a method in the LeafNode class that's trying
        // to retrieve an instance of a LeafNode (and instructions said that by this point we've
        // already traversed all the way down tree)
    }

    // See BPlusNode.getLeftmostLeaf.
    @Override
    public LeafNode getLeftmostLeaf() {
        // TODO(proj2): implement

        //Below is description of what this get function does:

        /**
         * n.getLeftmostLeaf() returns the leftmost leaf in the subtree rooted by n.
         * In the example above, inner.getLeftmostLeaf() would return leaf0, and
         * leaf1.getLeftmostLeaf() would return leaf1.
         */


        // Need to add in logic for finding left most, could implement in other classes though since "base case"

        return this; //Filler value, come back to this
    }

    // See BPlusNode.put.
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // TODO(proj2): implement

        //Below is description of what this get function does:

        /**
         * n.put(k, r) inserts the pair (k, r) into the subtree rooted by n. There
         * are two cases to consider:
         *
         *   Case 1: If inserting the pair (k, r) does NOT cause n to overflow, then
         *           Optional.empty() is returned.
         *   Case 2: If inserting the pair (k, r) does cause the node n to overflow,
         *           then n is split into a left and right node (described more
         *           below) and a pair (split_key, right_node_page_num) is returned
         *           where right_node_page_num is the page number of the newly
         *           created right node, and the value of split_key depends on
         *           whether n is an inner node or a leaf node (described more below).
         *
         * Now we explain how to split nodes and which split keys to return. Let's
         * take a look at an example. Consider inserting the key 4 into the example
         * tree above. No nodes overflow (i.e. we always hit case 1). The tree then
         * looks like this:
         *
         *                               inner
         *                               +----+----+----+----+
         *                               | 10 | 20 |    |    |
         *                               +----+----+----+----+
         *                              /     |     \
         *                         ____/      |      \____
         *                        /           |           \
         *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
         *   |  1 |  2 |  3 |  4 |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
         *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
         *   leaf0                  leaf1                  leaf2
         *
         * Now let's insert key 5 into the tree. Now, leaf0 overflows and creates a
         * new right sibling leaf3. d entries remain in the left node; d + 1 entries
         * are moved to the right node. DO NOT REDISTRIBUTE ENTRIES ANY OTHER WAY. In
         * our example, leaf0 and leaf3 would look like this:
         *
         *   +----+----+----+----+  +----+----+----+----+
         *   |  1 |  2 |    |    |->|  3 |  4 |  5 |    |
         *   +----+----+----+----+  +----+----+----+----+
         *   leaf0                  leaf3
         *
         * When a leaf splits, it returns the first entry in the right node as the
         * split key. In this example, 3 is the split key. After leaf0 splits, inner
         * inserts the new key and child pointer into itself and hits case 1 (i.e. it
         * does not overflow). The tree looks like this:
         *
         *                          inner
         *                          +--+--+--+--+
         *                          | 3|10|20|  |
         *                          +--+--+--+--+
         *                         /   |  |   \
         *                 _______/    |  |    \_________
         *                /            |   \             \
         *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
         *   | 1| 2|  |  |->| 3| 4| 5|  |->|11|12|13|  |->|21|22|23|  |
         *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
         *   leaf0          leaf3          leaf1          leaf2
         *
         * When an inner node splits, the first d entries are kept in the left node
         * and the last d entries are moved to the right node. The middle entry is
         * moved (not copied) up as the split key. For example, we would split the
         * following order 2 inner node
         *
         *   +---+---+---+---+
         *   | 1 | 2 | 3 | 4 | 5
         *   +---+---+---+---+
         *
         * into the following two inner nodes
         *
         *   +---+---+---+---+  +---+---+---+---+
         *   | 1 | 2 |   |   |  | 4 | 5 |   |   |
         *   +---+---+---+---+  +---+---+---+---+
         *
         * with a split key of 3.
         *
         * DO NOT redistribute entries in any other way besides what we have
         * described. For example, do not move entries between nodes to avoid
         * splitting.
         *
         * Our B+ trees do not support duplicate entries with the same key. If a
         * duplicate key is inserted into a leaf node, the tree is left unchanged
         * and a BPlusTreeException is raised.
         */


        


        //Case 2: If inserting the pair (k, r) does cause the node n to overflow,
        //then n is split into a left and right node (described more
        //below) and a pair (split_key, right_node_page_num) is returned
        //where right_node_page_num is the page number of the newly
        //created right node, and the value of split_key depends on
        //whether n is an inner node or a leaf node (described more below).

        //Handling Case 2 before Case 1, just because Case 2 adds info to results of Case 1
        if (keys.contains(key)) {
            throw new BPlusTreeException("Duplicate Key Error, Put Method LeafNode");
        }

        int new_index = InnerNode.numLessThanEqual(key, keys);
        /*
        java.util.Collections.binarySearch() method is a java.util.Collections class method
        that returns position of an object in a sorted list.

        If key is not present, the it returns "(-(insertion point) - 1)".
        The insertion point is defined as the point at which the key
        would be inserted into the list.
        */
        int orig_key_lengths = keys.size();
        int orig_rid_lengths = rids.size();
        keys.add(new_index, key);
        rids.add(new_index, rid);

        assert (orig_key_lengths+1) == (keys.size()): "Key Transfer incorrect, missing elements";
        assert (orig_rid_lengths+1) == (rids.size()): "RID Transfer incorrect, missing elements";

        int tree_order = metadata.getOrder();
        int max_values_leaf = 2 * tree_order;

        if (keys.size() <= max_values_leaf) {
            //this.sync();
            sync();
            return Optional.empty();
        } else {
            //Split Index
            int split_index = tree_order;
            DataBox split_key = keys.get(split_index);

            //Left Stuff
            List<DataBox> left_keys = new ArrayList<>();
            for (int i = 0; i < tree_order; i++) {
                DataBox key_ = keys.get(i);
                left_keys.add(key_);
            }
            List<RecordId> left_rids = new ArrayList<>();
            for (int i = 0; i < tree_order; i++) {
                RecordId rid_ = rids.get(i);
                left_rids.add(rid_);
            }

            //Right Stuff
            List<DataBox> right_keys = new ArrayList<>();
            for (int i = tree_order; i < max_values_leaf+1; i++) { //Extra indices could be problem, come back to check
                DataBox key_ = keys.get(i);
                right_keys.add(key_);
            }

            List<RecordId> right_rids = new ArrayList<>();
            for (int i = tree_order; i < max_values_leaf+1; i++) { //Extra indices could be problem, come back to check
                RecordId rid_ = rids.get(i);
                right_rids.add(rid_);
            }

            keys = left_keys;
            rids = left_rids; //Not sure about if should be children or this.children, same for above



            /*
            Constructor for Leaf Node:
            LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
             List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext)
            * */

            LeafNode result_LeafNode = new LeafNode(metadata, bufferManager, right_keys, right_rids, rightSibling, treeContext);

            Page result_pg = result_LeafNode.getPage();
            Long result_pgNum = result_pg.getPageNum();

            this.rightSibling = Optional.of(result_pgNum);


            Pair<DataBox, Long> result_pair = new Pair<>(split_key, result_pgNum);
            Optional<Pair<DataBox, Long>> result_pair_opt;
            if (result_pair == null) {
                result_pair_opt = Optional.empty();
            } else {
                result_pair_opt = Optional.of(result_pair);
            }
            result_LeafNode.sync();
            sync();
            return result_pair_opt;
        }




//        if (keys.size() > max_values_leaf) { //make sure this condition is correct
//            for (int i = 0; i < max_values_leaf; i++) {
//                DataBox orig_key = this.keys.remove(0);
//                RecordId orig_rid = this.rids.remove(0);
//                new_keys.add(i, orig_key);
//                new_rids.add(i, orig_rid);
//            }
//        }
//
//        keys.add(new_index, key);
//        rids.add(new_index, rid);
//
//        assert (orig_key_lengths+1) == (keys.size()): "Key Transfer incorrect, missing elements";
//        assert (orig_rid_lengths+1) == (rids.size()): "RID Transfer incorrect, missing elements";
//
//        //Case 1: If inserting the pair (k, r) does NOT cause n to overflow,
//        // then Optional.empty() is returned.
//
//        LeafNode new_leafnode = new LeafNode(
//                metadata, bufferManager, keys,
//                rids, rightSibling, treeContext
//        );
//        Page new_leafnode_pg = new_leafnode.getPage();
//        long new_leafnode_pg_number = new_leafnode_pg.getPageNum();
//        Optional<Long> rightSibling_optional;
//        if (new_leafnode_pg_number == -1) {
//            rightSibling_optional = Optional.empty();
//        } else {
//            rightSibling_optional = Optional.of(new_leafnode_pg_number);
//        }
//        this.rightSibling = rightSibling_optional;
//        this.sync();
//        sync();
//
//        Pair<DataBox, Long> newPair = new Pair<DataBox, Long>(keys.get(new_index), new_leafnode_pg_number);
//        Optional<Pair<DataBox, Long>> newPair_optional;
//        if (newPair == null) {
//            newPair_optional = Optional.empty();
//        } else {
//            newPair_optional = Optional.of(newPair);
//        }
//        newPair_optional = Optional.empty();
//        return newPair_optional;

    }

    // See BPlusNode.bulkLoad.
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
                                                  float fillFactor) {
        // TODO(proj2): implement

        //Below is description of what this get function does:

        /**
         * n.bulkLoad(data, fillFactor) bulk loads pairs of (k, r) from data into
         * the tree with the given fill factor.
         *
         * This method is very similar to n.put, with a couple of differences:
         *
         * 1. Leaf nodes do not fill up to 2*d+1 and split, but rather, fill up to
         * be 1 record more than fillFactor full, then "splits" by creating a right
         * sibling that contains just one record (leaving the original node with
         * the desired fill factor).
         *
         * 2. Inner nodes should repeatedly try to bulk load the rightmost child
         * until either the inner node is full (in which case it should split)
         * or there is no more data.
         *
         * fillFactor should ONLY be used for determining how full leaf nodes are
         * (not inner nodes), and calculations should round up, i.e. with d=5
         * and fillFactor=0.75, leaf nodes should be 8/10 full.
         *
         * You can assume that 0 < fillFactor <= 1 for testing purposes, and that
         * a fill factor outside of that range will result in undefined behavior
         * (you're free to handle those cases however you like).
         */
        int tree_order = metadata.getOrder();
        int max_values_leaf = (int) Math.ceil(2 * tree_order * fillFactor);


        while (data.hasNext()) {
            if (keys.size() >= max_values_leaf) {
                break;
            }
            Pair<DataBox, RecordId> newPair = data.next();
            DataBox newDataBox = newPair.getFirst();
            keys.add(newDataBox);
            RecordId newValue = newPair.getSecond();
            rids.add(newValue);
        }

        boolean more_to_come = data.hasNext();
        if (more_to_come) {
            List<DataBox> new_keys = new ArrayList<DataBox>();
            List<RecordId> new_rids = new ArrayList<RecordId>();
            Pair<DataBox, RecordId> newPair_extra = data.next();
            new_keys.add(newPair_extra.getFirst());
            new_rids.add(newPair_extra.getSecond());

            Optional<Long> rightSibling_empty = Optional.empty(); // Maybe look at for bulkload simple test case BPlusTree


            LeafNode new_leaf_node = new LeafNode(
                    metadata, bufferManager, new_keys,
                    new_rids, rightSibling_empty, treeContext
            );
            new_leaf_node.sync();
            this.sync();
            sync();

            Page result_pg = new_leaf_node.getPage();
            Long result_pgNum = result_pg.getPageNum();

            DataBox last_dbox = new_keys.get(0);

            Pair<DataBox, Long> result_pair = new Pair<DataBox, Long>(last_dbox, result_pgNum);
            Optional<Pair<DataBox, Long>> result_pair_opt;
            if (result_pair == null) {
                result_pair_opt = Optional.empty();
            } else {
                result_pair_opt = Optional.of(result_pair);
            }
            return result_pair_opt;
        }

        this.sync();
        sync();


        return Optional.empty();
    }

    // See BPlusNode.remove.
    @Override
    public void remove(DataBox key) {
        // TODO(proj2): implement

        if (keys.size() != 0) {
            for(int i=0;i<keys.size();i++) {
                if (keys.get(i).equals(key)) {
                    keys.remove(i);
                    rids.remove(i);
                    sync();
                    break;
                }
            }
        }

        return;
        

    }

    // Iterators ///////////////////////////////////////////////////////////////
    /** Return the record id associated with `key`. */
    Optional<RecordId> getKey(DataBox key) {
        int index = keys.indexOf(key);
        return index == -1 ? Optional.empty() : Optional.of(rids.get(index));
    }

    /**
     * Returns an iterator over the record ids of this leaf in ascending order of
     * their corresponding keys.
     */
    Iterator<RecordId> scanAll() {
        return rids.iterator();
    }

    /**
     * Returns an iterator over the record ids of this leaf that have a
     * corresponding key greater than or equal to `key`. The record ids are
     * returned in ascending order of their corresponding keys.
     */
    Iterator<RecordId> scanGreaterEqual(DataBox key) {
        int index = InnerNode.numLessThan(key, keys);
        return rids.subList(index, rids.size()).iterator();
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    /** Returns the right sibling of this leaf, if it has one. */
    Optional<LeafNode> getRightSibling() {
        if (!rightSibling.isPresent()) {
            return Optional.empty();
        }

        long pageNum = rightSibling.get();
        return Optional.of(LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum));
    }

    /** Serializes this leaf to its page. */
    private void sync() {
        page.pin();
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();
        }
    }

    // Just for testing.
    List<DataBox> getKeys() {
        return keys;
    }

    // Just for testing.
    List<RecordId> getRids() {
        return rids;
    }

    /**
     * Returns the largest number d such that the serialization of a LeafNode
     * with 2d entries will fit on a single page.
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // A leaf node with n entries takes up the following number of bytes:
        //
        //   1 + 8 + 4 + n * (keySize + ridSize)
        //
        // where
        //
        //   - 1 is the number of bytes used to store isLeaf,
        //   - 8 is the number of bytes used to store a sibling pointer,
        //   - 4 is the number of bytes used to store n,
        //   - keySize is the number of bytes used to store a DataBox of type
        //     keySchema, and
        //   - ridSize is the number of bytes of a RecordId.
        //
        // Solving the following equation
        //
        //   n * (keySize + ridSize) + 13 <= pageSizeInBytes
        //
        // we get
        //
        //   n = (pageSizeInBytes - 13) / (keySize + ridSize)
        //
        // The order d is half of n.
        int keySize = keySchema.getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + ridSize);
        return n / 2;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        String rightSibString = rightSibling.map(Object::toString).orElse("None");
        return String.format("LeafNode(pageNum=%s, keys=%s, rids=%s, rightSibling=%s)",
                page.getPageNum(), keys, rids, rightSibString);
    }

    @Override
    public String toSexp() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i).toString();
            String rid = rids.get(i).toSexp();
            ss.add(String.format("(%s %s)", key, rid));
        }
        return String.format("(%s)", String.join(" ", ss));
    }

    /**
     * Given a leaf with page number 1 and three (key, rid) pairs (0, (0, 0)),
     * (1, (1, 1)), and (2, (2, 2)), the corresponding dot fragment is:
     *
     *   node1[label = "{0: (0 0)|1: (1 1)|2: (2 2)}"];
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("%s: %s", keys.get(i), rids.get(i).toSexp()));
        }
        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        return String.format("  node%d[label = \"{%s}\"];", pageNum, s);
    }

    // Serialization ///////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // When we serialize a leaf node, we write:
        //
        //   a. the literal value 1 (1 byte) which indicates that this node is a
        //      leaf node,
        //   b. the page id (8 bytes) of our right sibling (or -1 if we don't have
        //      a right sibling),
        //   c. the number (4 bytes) of (key, rid) pairs this leaf node contains,
        //      and
        //   d. the (key, rid) pairs themselves.
        //
        // For example, the following bytes:
        //
        //   +----+-------------------------+-------------+----+-------------------------------+
        //   | 01 | 00 00 00 00 00 00 00 04 | 00 00 00 01 | 0 f3 | 00 00 00 00 00 00 00 03 00 01 |
        //   +----+-------------------------+-------------+----+-------------------------------+
        //    \__/ \_______________________/ \___________/ \__________________________________/
        //     a               b                   c                         d
        //
        // represent a leaf node with sibling on page 4 and a single (key, rid)
        // pair with key 3 and page id (3, 1).

        assert (keys.size() == rids.size());
        assert (keys.size() <= 2 * metadata.getOrder());

        // All sizes are in bytes.
        int isLeafSize = 1;
        int siblingSize = Long.BYTES;
        int lenSize = Integer.BYTES;
        int keySize = metadata.getKeySchema().getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int entriesSize = (keySize + ridSize) * keys.size();
        int size = isLeafSize + siblingSize + lenSize + entriesSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 1);
        buf.putLong(rightSibling.orElse(-1L));
        buf.putInt(keys.size());
        for (int i = 0; i < keys.size(); ++i) {
            buf.put(keys.get(i).toBytes());
            buf.put(rids.get(i).toBytes());
        }
        return buf.array();
    }

    /**
     * Loads a leaf node from page `pageNum`.
     */
    public static LeafNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                     LockContext treeContext, long pageNum) {
        // TODO(proj2): implement
        // Note: LeafNode has two constructors. To implement fromBytes be sure to
        // use the constructor that reuses an existing page instead of fetching a
        // brand new one.


        // Below is constructor I believe description is referring to, need to fill in arguments accordingly

        /*
        * private LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                List<DataBox> keys,
                List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
            }
        * */

        //Given: BPlusTreeMetadata metadata, BufferManager bufferManager, LockContext treeContext
        //Not Given: Page page, List<DataBox> keys, List<RecordId> rids, Optional<Long> rightSibling

        Page pg = bufferManager.fetchPage(treeContext, pageNum); //Know we need to get the buffer, so need to get Page object first
        Buffer bf = pg.getBuffer(); //Getting Buffer from Page, May Need to return ByteBuffer object

        // Below is how toBytes operates. Since we know these bytes are inputted left to right
        // we should be reading in the bytes from left to right, with knowledge of what each
        // byte represents according to its respective position
        /*
        buf.put((byte) 1);
        buf.putLong(rightSibling.orElse(-1L));
        buf.putInt(keys.size());
        for (int i = 0; i < keys.size(); ++i) {
            buf.put(keys.get(i).toBytes());
            buf.put(rids.get(i).toBytes());
        }
        return buf.array();
        * */

        byte first_byte = bf.get(); //What is the purpose of having this first byte? Is it just indication of start? Or canary value?
        long rightSibling_long = bf.getLong(); //Getting the page sibling id, which we need to convert to Optional type

        // How Optional works:

        /*
        For example, a call to get may not yield any value for a key that doesn't correspond to a record,
        in which case an Optional.empty() would be returned.
        If the key did correspond to a record, a populated Optional.of(RecordId(pageNum, entryNum)) would be returned instead.
        * */

        Optional<Long> rightSibling_optional;
        if (rightSibling_long == -1) {
            rightSibling_optional = Optional.empty();
        } else {
            rightSibling_optional = Optional.of(rightSibling_long);
        }

        int key_size = bf.getInt(); // Next value to read in is how many keys we want to be reading in
        List<DataBox> keys = new ArrayList<DataBox>();
        List<RecordId> rids = new ArrayList<RecordId>();

        for (int i = 0; i < key_size; ++i) {
            DataBox key_value = DataBox.fromBytes(bf, metadata.getKeySchema());
            keys.add(key_value);
            RecordId record_id = RecordId.fromBytes(bf);
            rids.add(record_id);
        }

        return new LeafNode(metadata, bufferManager, pg,
                keys, rids, rightSibling_optional, treeContext);

        //Test: TestLeafNode::testToAndFromBytes
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LeafNode)) {
            return false;
        }
        LeafNode n = (LeafNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
               keys.equals(n.keys) &&
               rids.equals(n.rids) &&
               rightSibling.equals(n.rightSibling);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, rids, rightSibling);
    }
}
