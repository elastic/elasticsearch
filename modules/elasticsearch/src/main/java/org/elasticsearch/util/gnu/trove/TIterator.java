/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.util.gnu.trove;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

/**
 * Abstract iterator class for THash implementations.  This class provides some
 * of the common iterator operations (hasNext(), remove()) and allows subclasses
 * to define the mechanism(s) for advancing the iterator and returning data.
 *
 * @author Eric D. Friedman
 * @version $Id: TIterator.java,v 1.3 2007/06/29 20:03:10 robeden Exp $
 */
abstract class TIterator {
    /**
     * the data structure this iterator traverses
     */
    protected final THash _hash;
    /**
     * the number of elements this iterator believes are in the
     * data structure it accesses.
     */
    protected int _expectedSize;
    /**
     * the index used for iteration.
     */
    protected int _index;

    /**
     * Create an instance of TIterator over the specified THash.
     */
    public TIterator(THash hash) {
        _hash = hash;
        _expectedSize = _hash.size();
        _index = _hash.capacity();
    }

    /**
     * Returns true if the iterator can be advanced past its current
     * location.
     *
     * @return a <code>boolean</code> value
     */
    public boolean hasNext() {
        return nextIndex() >= 0;
    }

    /**
     * Removes the last entry returned by the iterator.
     * Invoking this method more than once for a single entry
     * will leave the underlying data structure in a confused
     * state.
     */
    public void remove() {
        if (_expectedSize != _hash.size()) {
            throw new ConcurrentModificationException();
        }

        // Disable auto compaction during the remove. This is a workaround for bug 1642768.
        try {
            _hash.tempDisableAutoCompaction();
            _hash.removeAt(_index);
        }
        finally {
            _hash.reenableAutoCompaction(false);
        }

        _expectedSize--;
    }

    /**
     * Sets the internal <tt>index</tt> so that the `next' object
     * can be returned.
     */
    protected final void moveToNextIndex() {
        // doing the assignment && < 0 in one line shaves
        // 3 opcodes...
        if ((_index = nextIndex()) < 0) {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns the index of the next value in the data structure
     * or a negative value if the iterator is exhausted.
     *
     * @return an <code>int</code> value
     */
    abstract protected int nextIndex();
} // TIterator
