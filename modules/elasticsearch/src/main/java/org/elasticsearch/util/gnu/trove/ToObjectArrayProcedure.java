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
 * A procedure which stores each value it receives into a target array.
 * <p/>
 * Created: Sat Jan 12 10:13:42 2002
 *
 * @author Eric D. Friedman
 * @version $Id: ToObjectArrayProcedure.java,v 1.2 2006/11/10 23:27:57 robeden Exp $
 */

final class ToObjectArrayProcedure<T> implements TObjectProcedure<T> {
    private final T[] target;
    private int pos = 0;

    public ToObjectArrayProcedure(final T[] target) {
        this.target = target;
    }

    public final boolean execute(T value) {
        target[pos++] = value;
        return true;
    }
} // ToObjectArrayProcedure
