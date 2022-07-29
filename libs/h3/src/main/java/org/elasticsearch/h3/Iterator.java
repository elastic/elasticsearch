/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/uber/h3 which is licensed under the Apache 2.0 License.
 *
 * Copyright 2021 Uber Technologies, Inc.
 */
package org.elasticsearch.h3;

/**
 * Iterator structures and functions for the children of a cell.
 */
final class Iterator {
    /**
     * Invalid index used to indicate an error from latLngToCell and related
     * functions or missing data in arrays of H3 indices. Analogous to NaN in
     * floating point.
     */
    public static final long H3_NULL = 0;

    /**
     * The number of bits in a single H3 resolution digit.
     */
    private static final int H3_PER_DIGIT_OFFSET = 3;

    /**
     * IterCellsChildren: struct for iterating through the descendants of
     * a given cell.
     * <p>
     * Constructors:
     * <p>
     * Initialize with either `iterInitParent` or `iterInitBaseCellNum`.
     * `iterInitParent` sets up an iterator for all the children of a given
     * parent cell at a given resolution.
     * <p>
     * `iterInitBaseCellNum` sets up an iterator for children cells, given
     * a base cell number (0--121).
     * <p>
     * Iteration:
     * <p>
     * Step iterator with `iterStepChild`.
     * During the lifetime of the `IterCellsChildren`, the current iterate
     * is accessed via the `IterCellsChildren.h` member.
     * When the iterator is exhausted or if there was an error in initialization,
     * `IterCellsChildren.h` will be `H3_NULL` even after calling `iterStepChild`.
     */
    static class IterCellsChildren {
        long h;
        int _parentRes;  // parent resolution
        int _skipDigit;  // this digit skips `1` for pentagons

        IterCellsChildren(long h, int _parentRes, int _skipDigit) {
            this.h = h;
            this._parentRes = _parentRes;
            this._skipDigit = _skipDigit;
        }
    }

    /**
     * Create a fully nulled-out child iterator for when an iterator is exhausted.
     * This helps minimize the chance that a user will depend on the iterator
     * internal state after it's exhausted, like the child resolution, for
     * example.
     */
    private static IterCellsChildren nullIter() {
        return new IterCellsChildren(H3_NULL, -1, -1);
    }

    /**
     ## Logic for iterating through the children of a cell
     We'll describe the logic for ....
     - normal (non pentagon iteration)
     - pentagon iteration. define "pentagon digit"
     ### Cell Index Component Diagrams
     The lower 56 bits of an H3 Cell Index describe the following index components:
     - the cell resolution (4 bits)
     - the base cell number (7 bits)
     - the child cell digit for each resolution from 1 to 15 (3*15 = 45 bits)
     These are the bits we'll be focused on when iterating through child cells.
     To help describe the iteration logic, we'll use diagrams displaying the
     (decimal) values for each component like:
     child digit for resolution 2
     /
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 | ... |
     |-----|-------------|---|---|---|---|---|---|-----|
     |   9 |          17 | 5 | 3 | 0 | 6 | 2 | 1 | ... |
     ### Iteration through children of a hexagon (but not a pentagon)
     Iteration through the children of a *hexagon* (but not a pentagon)
     simply involves iterating through all the children values (0--6)
     for each child digit (up to the child's resolution).
     For example, suppose a resolution 3 hexagon index has the following
     components:
     parent resolution
     /
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 | ... |
     |-----|-------------|---|---|---|---|---|---|-----|
     |   3 |          17 | 3 | 5 | 1 | 7 | 7 | 7 | ... |
     The iteration through all children of resolution 6 would look like:
     parent res  child res
     /           /
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | ... |
     |-----|-------------|---|---|---|---|---|---|---|---|-----|
     | 6   |          17 | 3 | 5 | 1 | 0 | 0 | 0 | 7 | 7 | ... |
     | 6   |          17 | 3 | 5 | 1 | 0 | 0 | 1 | 7 | 7 | ... |
     | ... |             |   |   |   |   |   |   |   |   |     |
     | 6   |          17 | 3 | 5 | 1 | 0 | 0 | 6 | 7 | 7 | ... |
     | 6   |          17 | 3 | 5 | 1 | 0 | 1 | 0 | 7 | 7 | ... |
     | 6   |          17 | 3 | 5 | 1 | 0 | 1 | 1 | 7 | 7 | ... |
     | ... |             |   |   |   |   |   |   |   |   |     |
     | 6   |          17 | 3 | 5 | 1 | 6 | 6 | 6 | 7 | 7 | ... |
     ### Step sequence on a *pentagon* cell
     Pentagon cells have a base cell number (e.g., 97) corresponding to a
     resolution 0 pentagon, and have all zeros from digit 1 to the digit
     corresponding to the cell's resolution.
     (We'll drop the ellipses from now on, knowing that digits should contain
     7's beyond the cell resolution.)
     parent res      child res
     /               /
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 |
     |-----|-------------|---|---|---|---|---|---|
     |   6 |          97 | 0 | 0 | 0 | 0 | 0 | 0 |
     Iteration through children of a *pentagon* is almost the same
     as *hexagon* iteration, except that we skip the *first* 1 value
     that appears in the "skip digit". This corresponds to the fact
     that a pentagon only has 6 children, which are denoted with
     the numbers {0,2,3,4,5,6}.
     The skip digit starts at the child resolution position.
     When iterating through children more than one resolution below
     the parent, we move the skip digit to the left
     (up to the next coarser resolution) each time we skip the 1 value
     in that digit.
     Iteration would start like:
     parent res      child res
     /               /
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 |
     |-----|-------------|---|---|---|---|---|---|
     |   6 |          97 | 0 | 0 | 0 | 0 | 0 | 0 |
     \
     skip digit
     Noticing we skip the 1 value and move the skip digit,
     the next iterate would be:
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 |
     |-----|-------------|---|---|---|---|---|---|
     |   6 |          97 | 0 | 0 | 0 | 0 | 0 | 2 |
     \
     skip digit
     Iteration continues normally until we get to:
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 |
     |-----|-------------|---|---|---|---|---|---|
     |   6 |          97 | 0 | 0 | 0 | 0 | 0 | 6 |
     \
     skip digit
     which is followed by (skipping the 1):
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 |
     |-----|-------------|---|---|---|---|---|---|
     |   6 |          97 | 0 | 0 | 0 | 0 | 2 | 0 |
     \
     skip digit
     For the next iterate, we won't skip the `1` in the previous digit
     because it is no longer the skip digit:
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 |
     |-----|-------------|---|---|---|---|---|---|
     |   6 |          97 | 0 | 0 | 0 | 0 | 2 | 1 |
     \
     skip digit
     Iteration continues normally until we're right before the next skip
     digit:
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 |
     |-----|-------------|---|---|---|---|---|---|
     |   6 |          97 | 0 | 0 | 0 | 0 | 6 | 6 |
     \
     skip digit
     Which is followed by
     | res | base cell # | 1 | 2 | 3 | 4 | 5 | 6 |
     |-----|-------------|---|---|---|---|---|---|
     |   6 |          97 | 0 | 0 | 0 | 2 | 0 | 0 |
     \
     skip digit
     and so on.
     */

    /**
     * Initialize a IterCellsChildren struct representing the sequence giving
     * the children of cell `h` at resolution `childRes`.
     * <p>
     * At any point in the iteration, starting once
     * the struct is initialized, IterCellsChildren.h gives the current child.
     * <p>
     * Also, IterCellsChildren.h == H3_NULL when all the children have been iterated
     * through, or if the input to `iterInitParent` was invalid.
     */
    public static IterCellsChildren iterInitParent(long h, int childRes) {

        int parentRes = H3Index.H3_get_resolution(h);

        if (childRes < parentRes || childRes > Constants.MAX_H3_RES || h == H3_NULL) {
            return nullIter();
        }

        long newH = zeroIndexDigits(h, parentRes + 1, childRes);
        newH = H3Index.H3_set_resolution(newH, childRes);

        int _skipDigit;
        if (H3Index.H3_is_pentagon(newH)) {
            // The skip digit skips `1` for pentagons.
            // The "_skipDigit" moves to the left as we count up from the
            // child resolution to the parent resolution.
            _skipDigit = childRes;
        } else {
            // if not a pentagon, we can ignore "skip digit" logic
            _skipDigit = -1;
        }

        return new IterCellsChildren(newH, parentRes, _skipDigit);
    }

    /**
     * Step a IterCellsChildren to the next child cell.
     * When the iteration is over, IterCellsChildren.h will be H3_NULL.
     * Handles iterating through hexagon and pentagon cells.
     */
    public static void iterStepChild(IterCellsChildren it) {
        // once h == H3_NULL, the iterator returns an infinite sequence of H3_NULL
        if (it.h == H3_NULL) return;

        int childRes = H3Index.H3_get_resolution(it.h);

        incrementResDigit(it, childRes);

        for (int i = childRes; i >= it._parentRes; i--) {
            if (i == it._parentRes) {
                // if we're modifying the parent resolution digit, then we're done
                // *it = _null_iter();
                it.h = H3_NULL;
                return;
            }

            // PENTAGON_SKIPPED_DIGIT == 1
            if (i == it._skipDigit && getResDigit(it, i) == CoordIJK.Direction.PENTAGON_SKIPPED_DIGIT.digit()) {
                // Then we are iterating through the children of a pentagon cell.
                // All children of a pentagon have the property that the first
                // nonzero digit between the parent and child resolutions is
                // not 1.
                // I.e., we never see a sequence like 00001.
                // Thus, we skip the `1` in this digit.
                incrementResDigit(it, i);
                it._skipDigit -= 1;
                return;
            }

            // INVALID_DIGIT == 7
            if (getResDigit(it, i) == CoordIJK.Direction.INVALID_DIGIT.digit()) {
                incrementResDigit(it, i);  // zeros out it[i] and increments it[i-1] by 1
            } else {
                break;
            }
        }
    }

    // extract the `res` digit (0--7) of the current cell
    private static int getResDigit(IterCellsChildren it, int res) {
        return H3Index.H3_get_index_digit(it.h, res);
    }

    /**
     * Zero out index digits from start to end, inclusive.
     * No-op if start > end.
     */
    private static long zeroIndexDigits(long h, int start, int end) {
        if (start > end) {
            return h;
        }

        long m = 0;

        m = ~m;
        m <<= H3_PER_DIGIT_OFFSET * (end - start + 1);
        m = ~m;
        m <<= H3_PER_DIGIT_OFFSET * (Constants.MAX_H3_RES - end);
        m = ~m;

        return h & m;
    }

    // increment the digit (0--7) at location `res`
    private static void incrementResDigit(IterCellsChildren it, int res) {
        long val = 1;
        val <<= H3_PER_DIGIT_OFFSET * (Constants.MAX_H3_RES - res);
        it.h += val;
    }
}
