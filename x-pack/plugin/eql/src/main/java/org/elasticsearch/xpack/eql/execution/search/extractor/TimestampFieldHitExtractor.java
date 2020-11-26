/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search.extractor;

public class TimestampFieldHitExtractor extends FieldHitExtractor {

    public TimestampFieldHitExtractor(FieldHitExtractor target) {
        super(target.fieldName(), target.fullFieldName(), target.dataType(), target.zoneId(), target.useDocValues(), target.hitName(),
                target.arrayLeniency());
    }

    @Override
    protected Object parseDateString(Object values) {
        return Long.parseLong(values.toString());
    }
}
