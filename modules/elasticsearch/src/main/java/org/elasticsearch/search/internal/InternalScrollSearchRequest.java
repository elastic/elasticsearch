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

package org.elasticsearch.search.internal;

import org.elasticsearch.search.Scroll;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.search.Scroll.*;

/**
 * @author kimchy (Shay Banon)
 */
public class InternalScrollSearchRequest implements Streamable {

    private long id;

    private Scroll scroll;

    public InternalScrollSearchRequest() {
    }

    public InternalScrollSearchRequest(long id) {
        this.id = id;
    }

    public long id() {
        return id;
    }

    public Scroll scroll() {
        return scroll;
    }

    public InternalScrollSearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        id = in.readLong();
        if (in.readBoolean()) {
            scroll = readScroll(in);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeLong(id);
        if (scroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            scroll.writeTo(out);
        }
    }
}
