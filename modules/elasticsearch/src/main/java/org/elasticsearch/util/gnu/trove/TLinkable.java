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

import java.io.Serializable;


/**
 * Interface for Objects which can be inserted into a TLinkedList.
 * <p/>
 * <p>
 * Created: Sat Nov 10 15:23:41 2001
 * </p>
 *
 * @author Eric D. Friedman
 * @version $Id: TLinkable.java,v 1.2 2001/12/03 00:16:25 ericdf Exp $
 * @see org.elasticsearch.util.gnu.trove.TLinkedList
 */

public interface TLinkable extends Serializable {

    /**
     * Returns the linked list node after this one.
     *
     * @return a <code>TLinkable</code> value
     */
    public TLinkable getNext();

    /**
     * Returns the linked list node before this one.
     *
     * @return a <code>TLinkable</code> value
     */
    public TLinkable getPrevious();

    /**
     * Sets the linked list node after this one.
     *
     * @param linkable a <code>TLinkable</code> value
     */
    public void setNext(TLinkable linkable);

    /**
     * Sets the linked list node before this one.
     *
     * @param linkable a <code>TLinkable</code> value
     */
    public void setPrevious(TLinkable linkable);
}// TLinkable
