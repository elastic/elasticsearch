/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TypeGroup;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * {@link DataType#DOC_REF} is internal like {@link DataType#DOC_DATA_TYPE}: the planner adds it, users can neither
 * name it nor get it back.
 */
public class DocRefDataTypeTests extends ESTestCase {

    public void testUsersCannotNameIt() {
        assertThat(DataType.types(), not(hasItem(DataType.DOC_REF)));
        assertThat(DataType.fromTypeName("doc_ref"), nullValue());
        assertThat(DataType.fromNameOrAlias("doc_ref"), equalTo(DataType.UNSUPPORTED));
        assertThat(DataType.namesAndAliases(), not(hasItem("doc_ref")));
    }

    public void testFunctionsDoNotAcceptIt() {
        assertThat(TypeGroup.ALL.types(), not(hasItem(DataType.DOC_REF)));
        assertThat(TypeGroup.SORTABLE.types(), not(hasItem(DataType.DOC_REF)));
        assertFalse(DataType.isSortable(DataType.DOC_REF));
    }

    public void testReadFromName() throws IOException {
        assertThat(DataType.readFrom("DOC_REF"), sameInstance(DataType.DOC_REF));
        assertThat(DataType.readFrom("doc_ref"), sameInstance(DataType.DOC_REF));
    }

    public void testSupportedVersion() {
        TransportVersion version = DataType.DataTypesTransportVersions.ESQL_FETCH_PHASE_PLAN;
        boolean snapshot = Build.current().isSnapshot();
        assertTrue(DataType.DOC_REF.supportedVersion().supportedOn(version, snapshot));
        assertFalse(DataType.DOC_REF.supportedVersion().supportedOn(TransportVersionUtils.getPreviousVersion(version), snapshot));
    }

    /** Sending the type to a node that cannot read it is a planner bug, the sender fails before writing anything. */
    public void testWriteToOldNodeFails() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(DataType.DataTypesTransportVersions.ESQL_FETCH_PHASE_PLAN));
            QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class, () -> DataType.DOC_REF.writeTo(out));
            assertThat(e.getMessage(), containsString("doesn't understand data type [DOC_REF]"));
            assertThat(out.size(), equalTo(0));
        }
    }
}
