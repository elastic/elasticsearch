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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.NavigableSet;

/**
 * Only emits terms that exist in the parentTypes set.
 *
 * @elasticsearch.internal
 */
final class ParentChildFilteredTermsEnum extends FilteredTermsEnum {

    private final NavigableSet<BytesRef> parentTypes;

    private BytesRef seekTerm;
    private String type;
    private BytesRef id;

    ParentChildFilteredTermsEnum(TermsEnum tenum, NavigableSet<BytesRef> parentTypes) {
        super(tenum, true);
        this.parentTypes = parentTypes;
        this.seekTerm = parentTypes.isEmpty() ? null : parentTypes.first();
    }

    @Override
    protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
        BytesRef temp = seekTerm;
        seekTerm = null;
        return temp;
    }

    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
        if (parentTypes.isEmpty()) {
            return AcceptStatus.END;
        }

        BytesRef[] typeAndId = Uid.splitUidIntoTypeAndId(term);
        if (parentTypes.contains(typeAndId[0])) {
            type = typeAndId[0].utf8ToString();
            id = typeAndId[1];
            return AcceptStatus.YES;
        } else {
            BytesRef nextType = parentTypes.ceiling(typeAndId[0]);
            if (nextType == null) {
                return AcceptStatus.END;
            }
            seekTerm = nextType;
            return AcceptStatus.NO_AND_SEEK;
        }
    }

    public String type() {
        return type;
    }

    public BytesRef id() {
        return id;
    }
}
