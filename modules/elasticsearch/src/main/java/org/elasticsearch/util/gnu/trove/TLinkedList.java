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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractSequentialList;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * A LinkedList implementation which holds instances of type
 * <tt>TLinkable</tt>.
 * <p/>
 * <p>Using this implementation allows you to get java.util.LinkedList
 * behavior (a doubly linked list, with Iterators that support insert
 * and delete operations) without incurring the overhead of creating
 * <tt>Node</tt> wrapper objects for every element in your list.</p>
 * <p/>
 * <p>The requirement to achieve this time/space gain is that the
 * Objects stored in the List implement the <tt>TLinkable</tt>
 * interface.</p>
 * <p/>
 * <p>The limitations are that you cannot put the same object into
 * more than one list or more than once in the same list.  You must
 * also ensure that you only remove objects that are actually in the
 * list.  That is, if you have an object A and lists l1 and l2, you
 * must ensure that you invoke List.remove(A) on the correct list.  It
 * is also forbidden to invoke List.remove() with an unaffiliated
 * TLinkable (one that belongs to no list): this will destroy the list
 * you invoke it on.</p>
 * <p/>
 * <p>
 * Created: Sat Nov 10 15:25:10 2001
 * </p>
 *
 * @author Eric D. Friedman
 * @version $Id: TLinkedList.java,v 1.15 2009/03/31 19:43:14 robeden Exp $
 * @see org.elasticsearch.util.gnu.trove.TLinkable
 */

public class TLinkedList<T extends TLinkable> extends AbstractSequentialList<T>
        implements Externalizable {

    static final long serialVersionUID = 1L;


    /**
     * the head of the list
     */
    protected T _head;
    /**
     * the tail of the list
     */
    protected T _tail;
    /**
     * the number of elements in the list
     */
    protected int _size = 0;

    /**
     * Creates a new <code>TLinkedList</code> instance.
     */
    public TLinkedList() {
        super();
    }

    /**
     * Returns an iterator positioned at <tt>index</tt>.  Assuming
     * that the list has a value at that index, calling next() will
     * retrieve and advance the iterator.  Assuming that there is a
     * value before <tt>index</tt> in the list, calling previous()
     * will retrieve it (the value at index - 1) and move the iterator
     * to that position.  So, iterating from front to back starts at
     * 0; iterating from back to front starts at <tt>size()</tt>.
     *
     * @param index an <code>int</code> value
     * @return a <code>ListIterator</code> value
     */
    public ListIterator<T> listIterator(int index) {
        return new IteratorImpl(index);
    }

    /**
     * Returns the number of elements in the list.
     *
     * @return an <code>int</code> value
     */
    public int size() {
        return _size;
    }

    /**
     * Inserts <tt>linkable</tt> at index <tt>index</tt> in the list.
     * All values > index are shifted over one position to accommodate
     * the new addition.
     *
     * @param index    an <code>int</code> value
     * @param linkable an object of type TLinkable
     */
    public void add(int index, T linkable) {
        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException("index:" + index);
        }
        insert(index, linkable);
    }

    /**
     * Appends <tt>linkable</tt> to the end of the list.
     *
     * @param linkable an object of type TLinkable
     * @return always true
     */
    public boolean add(T linkable) {
        insert(_size, linkable);
        return true;
    }

    /**
     * Inserts <tt>linkable</tt> at the head of the list.
     *
     * @param linkable an object of type TLinkable
     */
    public void addFirst(T linkable) {
        insert(0, linkable);
    }

    /**
     * Adds <tt>linkable</tt> to the end of the list.
     *
     * @param linkable an object of type TLinkable
     */
    public void addLast(T linkable) {
        insert(size(), linkable);
    }

    /**
     * Empties the list.
     */
    public void clear() {
        if (null != _head) {
            for (TLinkable link = _head.getNext();
                 link != null;
                 link = link.getNext()) {
                TLinkable prev = link.getPrevious();
                prev.setNext(null);
                link.setPrevious(null);
            }
            _head = _tail = null;
        }
        _size = 0;
    }

    /**
     * Copies the list's contents into a native array.  This will be a
     * shallow copy: the Tlinkable instances in the Object[] array
     * have links to one another: changing those will put this list
     * into an unpredictable state.  Holding a reference to one
     * element in the list will prevent the others from being garbage
     * collected unless you clear the next/previous links.  <b>Caveat
     * programmer!</b>
     *
     * @return an <code>Object[]</code> value
     */
    public Object[] toArray() {
        Object[] o = new Object[_size];
        int i = 0;
        for (TLinkable link = _head; link != null; link = link.getNext()) {
            o[i++] = link;
        }
        return o;
    }

    /**
     * Copies the list to a native array, destroying the next/previous
     * links as the copy is made.  This list will be emptied after the
     * copy (as if clear() had been invoked).  The Object[] array
     * returned will contain TLinkables that do <b>not</b> hold
     * references to one another and so are less likely to be the
     * cause of memory leaks.
     *
     * @return an <code>Object[]</code> value
     */
    public Object[] toUnlinkedArray() {
        Object[] o = new Object[_size];
        int i = 0;
        for (T link = _head, tmp = null; link != null; i++) {
            o[i] = link;
            tmp = link;
            link = (T) link.getNext();
            tmp.setNext(null); // clear the links
            tmp.setPrevious(null);
        }
        _size = 0;              // clear the list
        _head = _tail = null;
        return o;
    }

    /**
     * A linear search for <tt>o</tt> in the list.
     *
     * @param o an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean contains(Object o) {
        for (TLinkable link = _head; link != null; link = link.getNext()) {
            if (o.equals(link)) {
                return true;
            }
        }
        return false;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public T get(int index) {
        // Blow out for bogus values
        if (index < 0 || index >= _size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + _size);
        }

        // Determine if it's better to get there from the front or the back
        if (index > (_size >> 1)) {
            int position = _size - 1;
            T node = _tail;

            while (position > index) {
                node = (T) node.getPrevious();
                position--;
            }

            return node;
        } else {
            int position = 0;
            T node = _head;

            while (position < index) {
                node = (T) node.getNext();
                position++;
            }

            return node;
        }
    }


    /**
     * Returns the head of the list
     *
     * @return an <code>Object</code> value
     */
    public T getFirst() {
        return _head;
    }

    /**
     * Returns the tail of the list.
     *
     * @return an <code>Object</code> value
     */
    public T getLast() {
        return _tail;
    }


    /**
     * Return the node following the given node. This method exists for two reasons:
     * <ol>
     * <li>It's really not recommended that the methods implemented by TLinkable be
     * called directly since they're used internally by this class.</li>
     * <li>This solves problems arising from generics when working with the linked
     * objects directly.</li>
     * </ol>
     * <p/>
     * NOTE: this should only be used with nodes contained in the list. The results are
     * undefined with anything else.
     */
    public T getNext(T current) {
        return (T) current.getNext();
    }

    /**
     * Return the node preceding the given node. This method exists for two reasons:
     * <ol>
     * <li>It's really not recommended that the methods implemented by TLinkable be
     * called directly since they're used internally by this class.</li>
     * <li>This solves problems arising from generics when working with the linked
     * objects directly.</li>
     * </ol>
     * <p/>
     * NOTE: this should only be used with nodes contained in the list. The results are
     * undefined with anything else.
     */
    public T getPrevious(T current) {
        return (T) current.getPrevious();
    }


    /**
     * Remove and return the first element in the list.
     *
     * @return an <code>Object</code> value
     */
    public T removeFirst() {
        T o = _head;

        if (o == null) return null;

        T n = (T) o.getNext();
        o.setNext(null);

        if (null != n) {
            n.setPrevious(null);
        }

        _head = n;
        if (--_size == 0) {
            _tail = null;
        }
        return o;
    }

    /**
     * Remove and return the last element in the list.
     *
     * @return an <code>Object</code> value
     */
    public T removeLast() {
        T o = _tail;

        if (o == null) return null;

        T prev = (T) o.getPrevious();
        o.setPrevious(null);

        if (null != prev) {
            prev.setNext(null);
        }
        _tail = prev;
        if (--_size == 0) {
            _head = null;
        }
        return o;
    }

    /**
     * Implementation of index-based list insertions.
     *
     * @param index    an <code>int</code> value
     * @param linkable an object of type TLinkable
     */
    protected void insert(int index, T linkable) {
        T newLink = linkable;

        if (_size == 0) {
            _head = _tail = newLink; // first insertion
        } else if (index == 0) {
            newLink.setNext(_head); // insert at front
            _head.setPrevious(newLink);
            _head = newLink;
        } else if (index == _size) { // insert at back
            _tail.setNext(newLink);
            newLink.setPrevious(_tail);
            _tail = newLink;
        } else {
            T node = get(index);

            T before = (T) node.getPrevious();
            if (before != null) before.setNext(linkable);

            linkable.setPrevious(before);
            linkable.setNext(node);
            node.setPrevious(linkable);
        }
        _size++;
    }

    /**
     * Removes the specified element from the list.  Note that
     * it is the caller's responsibility to ensure that the
     * element does, in fact, belong to this list and not another
     * instance of TLinkedList.
     *
     * @param o a TLinkable element already inserted in this list.
     * @return true if the element was a TLinkable and removed
     */
    public boolean remove(Object o) {
        if (o instanceof TLinkable) {
            T p, n;
            TLinkable link = (TLinkable) o;

            p = (T) link.getPrevious();
            n = (T) link.getNext();

            if (n == null && p == null) { // emptying the list
                // It's possible this object is not something that's in the list. So,
                // make sure it's the head if it doesn't point to anything. This solves
                // problems caused by removing something multiple times.
                if (o != _head) return false;

                _head = _tail = null;
            } else if (n == null) { // this is the tail
                // make previous the new tail
                link.setPrevious(null);
                p.setNext(null);
                _tail = p;
            } else if (p == null) { // this is the head
                // make next the new head
                link.setNext(null);
                n.setPrevious(null);
                _head = n;
            } else {            // somewhere in the middle
                p.setNext(n);
                n.setPrevious(p);
                link.setNext(null);
                link.setPrevious(null);
            }

            _size--;            // reduce size of list
            return true;
        } else {
            return false;
        }
    }

    /**
     * Inserts newElement into the list immediately before current.
     * All elements to the right of and including current are shifted
     * over.
     *
     * @param current    a <code>TLinkable</code> value currently in the list.
     * @param newElement a <code>TLinkable</code> value to be added to
     *                   the list.
     */
    public void addBefore(T current, T newElement) {
        if (current == _head) {
            addFirst(newElement);
        } else if (current == null) {
            addLast(newElement);
        } else {
            TLinkable p = current.getPrevious();
            newElement.setNext(current);
            p.setNext(newElement);
            newElement.setPrevious(p);
            current.setPrevious(newElement);
            _size++;
        }
    }

    /**
     * Inserts newElement into the list immediately after current.
     * All elements to the left of and including current are shifted
     * over.
     *
     * @param current    a <code>TLinkable</code> value currently in the list.
     * @param newElement a <code>TLinkable</code> value to be added to
     *                   the list.
     */
    public void addAfter(T current, T newElement) {
        if (current == _tail) {
            addLast(newElement);
        } else if (current == null) {
            addFirst(newElement);
        } else {
            TLinkable n = current.getNext();
            newElement.setPrevious(current);
            newElement.setNext(n);
            current.setNext(newElement);
            n.setPrevious(newElement);
            _size++;
        }
    }


    /**
     * Executes <tt>procedure</tt> for each entry in the list.
     *
     * @param procedure a <code>TObjectProcedure</code> value
     * @return false if the loop over the values terminated because
     *         the procedure returned false for some value.
     */
    public boolean forEachValue(TObjectProcedure<T> procedure) {
        T node = _head;
        while (node != null) {
            boolean keep_going = procedure.execute(node);
            if (!keep_going) return false;

            node = (T) node.getNext();
        }

        return true;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        // VERSION
        out.writeByte(0);

        // NUMBER OF ENTRIES
        out.writeInt(_size);

        // HEAD
        out.writeObject(_head);

        // TAIL
        out.writeObject(_tail);
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {

        // VERSION
        in.readByte();

        // NUMBER OF ENTRIED
        _size = in.readInt();

        // HEAD
        _head = (T) in.readObject();

        // TAIL
        _tail = (T) in.readObject();
    }


    /**
     * A ListIterator that supports additions and deletions.
     */
    protected final class IteratorImpl implements ListIterator<T> {
        private int _nextIndex = 0;
        private T _next;
        private T _lastReturned;

        /**
         * Creates a new <code>Iterator</code> instance positioned at
         * <tt>index</tt>.
         *
         * @param position an <code>int</code> value
         */
        IteratorImpl(int position) {
            if (position < 0 || position > _size) {
                throw new IndexOutOfBoundsException();
            }

            _nextIndex = position;
            if (position == 0) {
                _next = _head;
            } else if (position == _size) {
                _next = null;
            } else if (position < (_size >> 1)) {
                int pos = 0;
                for (_next = _head; pos < position; pos++) {
                    _next = (T) _next.getNext();
                }
            } else {
                int pos = _size - 1;
                for (_next = _tail; pos > position; pos--) {
                    _next = (T) _next.getPrevious();
                }
            }
        }

        /**
         * Insert <tt>linkable</tt> at the current position of the iterator.
         * Calling next() after add() will return the added object.
         *
         * @param linkable an object of type TLinkable
         */
        public final void add(T linkable) {
            _lastReturned = null;
            _nextIndex++;

            if (_size == 0) {
                TLinkedList.this.add(linkable);
            } else {
                TLinkedList.this.addBefore(_next, linkable);
            }
        }

        /**
         * True if a call to next() will return an object.
         *
         * @return a <code>boolean</code> value
         */
        public final boolean hasNext() {
            return _nextIndex != _size;
        }

        /**
         * True if a call to previous() will return a value.
         *
         * @return a <code>boolean</code> value
         */
        public final boolean hasPrevious() {
            return _nextIndex != 0;
        }

        /**
         * Returns the value at the Iterator's index and advances the
         * iterator.
         *
         * @return an <code>Object</code> value
         * @throws NoSuchElementException if there is no next element
         */
        public final T next() {
            if (_nextIndex == _size) {
                throw new NoSuchElementException();
            }

            _lastReturned = _next;
            _next = (T) _next.getNext();
            _nextIndex++;
            return _lastReturned;
        }

        /**
         * returns the index of the next node in the list (the
         * one that would be returned by a call to next()).
         *
         * @return an <code>int</code> value
         */
        public final int nextIndex() {
            return _nextIndex;
        }

        /**
         * Returns the value before the Iterator's index and moves the
         * iterator back one index.
         *
         * @return an <code>Object</code> value
         * @throws NoSuchElementException if there is no previous element.
         */
        public final T previous() {
            if (_nextIndex == 0) {
                throw new NoSuchElementException();
            }

            if (_nextIndex == _size) {
                _lastReturned = _next = _tail;
            } else {
                _lastReturned = _next = (T) _next.getPrevious();
            }

            _nextIndex--;
            return _lastReturned;
        }

        /**
         * Returns the previous element's index.
         *
         * @return an <code>int</code> value
         */
        public final int previousIndex() {
            return _nextIndex - 1;
        }

        /**
         * Removes the current element in the list and shrinks its
         * size accordingly.
         *
         * @throws IllegalStateException neither next nor previous
         *                               have been invoked, or remove or add have been invoked after
         *                               the last invocation of next or previous.
         */
        public final void remove() {
            if (_lastReturned == null) {
                throw new IllegalStateException("must invoke next or previous before invoking remove");
            }

            if (_lastReturned != _next) {
                _nextIndex--;
            }
            _next = (T) _lastReturned.getNext();
            TLinkedList.this.remove(_lastReturned);
            _lastReturned = null;
        }

        /**
         * Replaces the current element in the list with
         * <tt>linkable</tt>
         *
         * @param linkable an object of type TLinkable
         */
        public final void set(T linkable) {
            if (_lastReturned == null) {
                throw new IllegalStateException();
            }
            T l = linkable;

            // need to check both, since this could be the only
            // element in the list.
            if (_lastReturned == _head) {
                _head = l;
            }

            if (_lastReturned == _tail) {
                _tail = l;
            }

            swap(_lastReturned, l);
            _lastReturned = l;
        }

        /**
         * Replace from with to in the list.
         *
         * @param from a <code>TLinkable</code> value
         * @param to   a <code>TLinkable</code> value
         */
        private void swap(T from, T to) {
            T p = (T) from.getPrevious();
            T n = (T) from.getNext();

            if (null != p) {
                to.setPrevious(p);
                p.setNext(to);
            }
            if (null != n) {
                to.setNext(n);
                n.setPrevious(to);
            }
            from.setNext(null);
            from.setPrevious(null);
        }
    }
} // TLinkedList
