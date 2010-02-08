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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.Actions.*;
import static org.elasticsearch.util.TimeValue.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;

/**
 * @author kimchy (Shay Banon)
 */
public class CreateIndexRequest extends MasterNodeOperationRequest {

    private String index;

    private Settings settings = EMPTY_SETTINGS;

    private TimeValue timeout = new TimeValue(10, TimeUnit.SECONDS);

    public CreateIndexRequest(String index) {
        this(index, EMPTY_SETTINGS);
    }

    public CreateIndexRequest(String index, Settings settings) {
        this.index = index;
        this.settings = settings;
    }

    CreateIndexRequest() {
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    String index() {
        return index;
    }

    Settings settings() {
        return settings;
    }

    public CreateIndexRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    TimeValue timeout() {
        return timeout;
    }

    public CreateIndexRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        index = in.readUTF();
        settings = readSettingsFromStream(in);
        timeout = readTimeValue(in);
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(index);
        writeSettingsToStream(settings, out);
        timeout.writeTo(out);
    }
}