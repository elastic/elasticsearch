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

/**
 * Adapter for TLinkable interface which implements the interface and can
 * therefore be extended trivially to create TLinkable objects without
 * having to implement the obvious.
 * <p/>
 * <p>
 * Created: Thurs Nov 15 16:25:00 2001
 * </p>
 *
 * @author Jason Baldridge
 * @version $Id: TLinkableAdapter.java,v 1.1 2006/11/10 23:27:56 robeden Exp $
 * @see org.elasticsearch.util.gnu.trove.TLinkedList
 */

public class TLinkableAdapter implements TLinkable {
    TLinkable _previous, _next;

    /**
     * Returns the linked list node after this one.
     *
     * @return a <code>TLinkable</code> value
     */
    public TLinkable getNext() {
        return _next;
    }

    /**
     * Returns the linked list node before this one.
     *
     * @return a <code>TLinkable</code> value
     */
    public TLinkable getPrevious() {
        return _previous;
    }

    /**
     * Sets the linked list node after this one.
     *
     * @param linkable a <code>TLinkable</code> value
     */
    public void setNext(TLinkable linkable) {
        _next = linkable;
    }

    /**
     * Sets the linked list node before this one.
     *
     * @param linkable a <code>TLinkable</code> value
     */
    public void setPrevious(TLinkable linkable) {
        _previous = linkable;
    }
}
