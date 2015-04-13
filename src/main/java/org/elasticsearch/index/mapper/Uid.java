/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public final class Uid {

    public static final char DELIMITER = '#';
    public static final byte DELIMITER_BYTE = 0x23;
    public static final BytesRef DELIMITER_BYTES = new BytesRef(new byte[]{DELIMITER_BYTE});

    private final String type;

    private final String id;

    public Uid(String type, String id) {
        this.type = type;
        this.id = id;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Uid uid = (Uid) o;

        if (id != null ? !id.equals(uid.id) : uid.id != null) return false;
        if (type != null ? !type.equals(uid.type) : uid.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return createUid(type, id);
    }

    public BytesRef toBytesRef() {
        return createUidAsBytes(type, id);
    }

    public static BytesRef typePrefixAsBytes(BytesRef type) {
        BytesRefBuilder bytesRef = new BytesRefBuilder();
        bytesRef.append(type);
        bytesRef.append(DELIMITER_BYTES);
        return bytesRef.toBytesRef();
    }

    public static Uid createUid(String uid) {
        int delimiterIndex = uid.indexOf(DELIMITER); // type is not allowed to have # in it..., ids can
        return new Uid(uid.substring(0, delimiterIndex), uid.substring(delimiterIndex + 1));
    }

    public static BytesRef[] createUids(List<? extends DocumentRequest> requests) {
        BytesRef[] uids = new BytesRef[requests.size()];
        int idx = 0;
        for (DocumentRequest item : requests) {
            uids[idx++] = createUidAsBytes(item.type(), item.id());
        }
        return uids;
    }

    public static BytesRef createUidAsBytes(String type, String id) {
        return createUidAsBytes(new BytesRef(type), new BytesRef(id));
    }

    public static BytesRef createUidAsBytes(String type, BytesRef id) {
        return createUidAsBytes(new BytesRef(type), id);
    }

    public static BytesRef createUidAsBytes(BytesRef type, BytesRef id) {
        final BytesRef ref = new BytesRef(type.length + 1 + id.length);
        System.arraycopy(type.bytes, type.offset, ref.bytes, 0, type.length);
        ref.offset = type.length;
        ref.bytes[ref.offset++] = DELIMITER_BYTE;
        System.arraycopy(id.bytes, id.offset, ref.bytes, ref.offset, id.length);
        ref.offset = 0;
        ref.length = ref.bytes.length;
        return ref;
    }

    public static BytesRef createUidAsBytes(BytesRef type, BytesRef id, BytesRefBuilder spare) {
        spare.copyBytes(type);
        spare.append(DELIMITER_BYTES);
        spare.append(id);
        return spare.get();
    }

    public static BytesRef[] createTypeUids(Collection<String> types, Object ids) {
        return createTypeUids(types, Collections.singletonList(ids));
    }

    public static BytesRef[] createTypeUids(Collection<String> types, List<? extends Object> ids) {
        final int numIds = ids.size();
        BytesRef[] uids = new BytesRef[types.size() * ids.size()];
        BytesRefBuilder typeBytes = new BytesRefBuilder();
        BytesRefBuilder idBytes = new BytesRefBuilder();
        int index = 0;
        for (String type : types) {
            typeBytes.copyChars(type);
            for (int i = 0; i < numIds; i++, index++) {
                uids[index] = Uid.createUidAsBytes(typeBytes.get(), BytesRefs.toBytesRef(ids.get(i), idBytes));
            }
        }
        return uids;
    }

    public static String createUid(String type, String id) {
        return createUid(new StringBuilder(), type, id);
    }

    public static String createUid(StringBuilder sb, String type, String id) {
        return sb.append(type).append(DELIMITER).append(id).toString();
    }

    public static boolean hasDelimiter(BytesRef uid) {
        final int limit = uid.offset + uid.length;
        for (int i = uid.offset; i < limit; i++) {
            if (uid.bytes[i] == DELIMITER_BYTE) { // 0x23 is equal to '#'
                return true;
            }
        }
        return false;
    }

    public static BytesRef[] splitUidIntoTypeAndId(BytesRef uid) {
        int loc = -1;
        final int limit = uid.offset + uid.length;
        for (int i = uid.offset; i < limit; i++) {
            if (uid.bytes[i] == DELIMITER_BYTE) { // 0x23 is equal to '#'
                loc = i;
                break;
            }
        }

        if (loc == -1) {
            return null;
        }

        int idStart = loc + 1;
        return new BytesRef[] {
                new BytesRef(uid.bytes, uid.offset, loc - uid.offset),
                new BytesRef(uid.bytes, idStart, limit - idStart)
        };
    }

}
