/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.IdFieldMapper;

/**
 * A {@link StoredFieldVisitor} that captures the raw stored {@code _id} bytes without decoding them.
 * Slice-enabled indices store the compound identity term (id ++ slice ++ length byte) for both live docs and
 * tombstones; the generic stored-field path decodes {@code _id} via {@code Uid.decodeId} which garbles compound
 * bytes. This visitor captures the raw bytes so callers can use them as the uid key or decode via
 * {@link org.elasticsearch.index.mapper.IdFieldMapper#decodeIdentity}.
 */
final class RawIdVisitor extends StoredFieldVisitor {
    BytesRef idBytes;

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        return IdFieldMapper.NAME.equals(fieldInfo.name) ? Status.YES : Status.NO;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        idBytes = new BytesRef(value);
    }
}
