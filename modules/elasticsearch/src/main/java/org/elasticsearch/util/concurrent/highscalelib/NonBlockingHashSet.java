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

/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.elasticsearch.util.concurrent.highscalelib;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;


/**
 * A simple wrapper around {@link NonBlockingHashMap} making it implement the
 * {@link Set} interface.  All operations are Non-Blocking and multi-thread safe.
 *
 * @author Cliff Click
 * @since 1.5
 */


public class NonBlockingHashSet<E> extends AbstractSet<E> implements Serializable {
    private static final Object V = "";

    private final NonBlockingHashMap<E, Object> _map;

    /**
     * Make a new empty {@link NonBlockingHashSet}.
     */
    public NonBlockingHashSet() {
        super();
        _map = new NonBlockingHashMap<E, Object>();
    }

    /**
     * Add {@code o} to the set.
     *
     * @return <tt>true</tt> if {@code o} was added to the set, <tt>false</tt>
     *         if {@code o} was already in the set.
     */
    public boolean add(final E o) {
        return _map.putIfAbsent(o, V) != V;
    }

    /**
     * @return <tt>true</tt> if {@code o} is in the set.
     */
    public boolean contains(final Object o) {
        return _map.containsKey(o);
    }

    /**
     * Remove {@code o} from the set.
     *
     * @return <tt>true</tt> if {@code o} was removed to the set, <tt>false</tt>
     *         if {@code o} was not in the set.
     */
    public boolean remove(final Object o) {
        return _map.remove(o) == V;
    }

    /**
     * Current count of elements in the set.  Due to concurrent racing updates,
     * the size is only ever approximate.  Updates due to the calling thread are
     * immediately visible to calling thread.
     *
     * @return count of elements.
     */
    public int size() {
        return _map.size();
    }

    /**
     * Empty the set.
     */
    public void clear() {
        _map.clear();
    }

    public Iterator<E> iterator() {
        return _map.keySet().iterator();
    }

    // ---

    /**
     * Atomically make the set immutable.  Future calls to mutate will throw an
     * IllegalStateException.  Existing mutator calls in other threads racing
     * with this thread and will either throw IllegalStateException or their
     * update will be visible to this thread.  This implies that a simple flag
     * cannot make the Set immutable, because a late-arriving update in another
     * thread might see immutable flag not set yet, then mutate the Set after
     * the {@link #readOnly} call returns.  This call can be called concurrently
     * (and indeed until the operation completes, all calls on the Set from any
     * thread either complete normally or end up calling {@link #readOnly}
     * internally).
     * <p/>
     * <p> This call is useful in debugging multi-threaded programs where the
     * Set is constructed in parallel, but construction completes after some
     * time; and after construction the Set is only read.  Making the Set
     * read-only will cause updates arriving after construction is supposedly
     * complete to throw an {@link IllegalStateException}.
     */

    // (1) call _map's immutable() call
    // (2) get snapshot
    // (3) CAS down a local map, power-of-2 larger than _map.size()+1/8th
    // (4) start @ random, visit all snapshot, insert live keys
    // (5) CAS _map to null, needs happens-after (4)
    // (6) if Set call sees _map is null, needs happens-after (4) for readers
    public void readOnly() {
        throw new RuntimeException("Unimplemented");
    }
}
