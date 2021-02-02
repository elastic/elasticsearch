/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;

public final class IdOnlyFieldVisitor extends StoredFieldVisitor {
    private String id = null;
    private boolean visited = false;

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        if (visited) {
            return Status.STOP;
        }
        if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
            visited = true;
            return Status.YES;
        } else {
            return Status.NO;
        }
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        assert IdFieldMapper.NAME.equals(fieldInfo.name) : fieldInfo;
        if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
            id = Uid.decodeId(value);
        }
    }

    public String getId() {
        return id;
    }

    public void reset() {
        id = null;
        visited = false;
    }
}
