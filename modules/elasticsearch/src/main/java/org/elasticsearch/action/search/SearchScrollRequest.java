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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.search.Scroll;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.action.Actions.*;
import static org.elasticsearch.search.Scroll.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SearchScrollRequest implements ActionRequest {

    private String scrollId;

    private Scroll scroll;

    public SearchScrollRequest() {
    }

    public SearchScrollRequest(String scrollId) {
        this.scrollId = scrollId;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (scrollId == null) {
            validationException = addValidationError("scrollId is missing", validationException);
        }
        return validationException;
    }

    @Override public boolean listenerThreaded() {
        // TODO threaded
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override public ActionRequest listenerThreaded(boolean threadedListener) {
        // TODO threaded
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String scrollId() {
        return scrollId;
    }

    public Scroll scroll() {
        return scroll;
    }

    public void scroll(Scroll scroll) {
        this.scroll = scroll;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        scrollId = in.readUTF();
        if (in.readBoolean()) {
            scroll = readScroll(in);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(scrollId);
        if (scroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            scroll.writeTo(out);
        }
    }
}
