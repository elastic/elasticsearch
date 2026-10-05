/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LayoutTests extends ESTestCase {
    /**
     * An operator that turns {@code _doc} into a document reference keeps the channel. Operators above it must see the
     * new type, otherwise a TopN would pick the encoder of the old one.
     */
    public void testReplaceWithType() {
        NameId doc = new NameId();
        NameId docRef = new NameId();
        NameId other = new NameId();
        Layout.Builder builder = new Layout.Builder();
        builder.append(new Layout.ChannelSet(new HashSet<>(Set.of(other)), DataType.LONG));
        builder.append(new Layout.ChannelSet(new HashSet<>(Set.of(doc)), DataType.DOC_DATA_TYPE));
        builder.replace(doc, docRef, DataType.DOC_REF);
        Layout layout = builder.build();

        assertThat(layout.get(docRef), equalTo(new Layout.ChannelAndType(1, DataType.DOC_REF)));
        assertThat(layout.get(doc), nullValue());
        assertThat(layout.get(other), equalTo(new Layout.ChannelAndType(0, DataType.LONG)));
    }
}
