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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class AliasAction implements Streamable {

    public static enum Type {
        ADD((byte) 0),
        REMOVE((byte) 1);

        private final byte value;

        Type(byte value) {
            this.value = value;
        }

        public byte value() {
            return value;
        }

        public static Type fromValue(byte value) {
            if (value == 0) {
                return ADD;
            } else if (value == 1) {
                return REMOVE;
            } else {
                throw new ElasticSearchIllegalArgumentException("No type for action [" + value + "]");
            }
        }
    }

    private Type actionType;

    private String index;

    private String alias;

    private AliasAction() {

    }

    public AliasAction(Type actionType, String index, String alias) {
        this.actionType = actionType;
        this.index = index;
        this.alias = alias;
    }

    public Type actionType() {
        return actionType;
    }

    public String index() {
        return index;
    }

    public String alias() {
        return alias;
    }

    public static AliasAction readAliasAction(StreamInput in) throws IOException {
        AliasAction aliasAction = new AliasAction();
        aliasAction.readFrom(in);
        return aliasAction;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        actionType = Type.fromValue(in.readByte());
        index = in.readUTF();
        alias = in.readUTF();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(actionType.value());
        out.writeUTF(index);
        out.writeUTF(alias);
    }
}
