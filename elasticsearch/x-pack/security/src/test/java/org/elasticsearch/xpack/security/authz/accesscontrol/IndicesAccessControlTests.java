/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissions;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link IndicesAccessControl}
 */
public class IndicesAccessControlTests extends ESTestCase {

    public void testEmptyIndicesAccessControl() {
        IndicesAccessControl indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        assertThat(indicesAccessControl.isGranted(), is(true));
        assertThat(indicesAccessControl.getIndexPermissions(randomAsciiOfLengthBetween(3,20)), nullValue());
    }

    public void testMergeFields() {
        IndexAccessControl indexAccessControl = new IndexAccessControl(true, new FieldPermissions(new String[]{"a", "c"}, null), null);
        IndexAccessControl other = new IndexAccessControl(true,new FieldPermissions(new String[]{"b"}, null), null);

        IndexAccessControl merge1 = indexAccessControl.merge(other);
        assertTrue(merge1.getFieldPermissions().grantsAccessTo("a"));
        assertTrue(merge1.getFieldPermissions().grantsAccessTo("b"));
        assertTrue(merge1.getFieldPermissions().grantsAccessTo("c"));
        assertTrue(merge1.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge1.isGranted(), is(true));
        assertThat(merge1.getQueries(), nullValue());

        IndexAccessControl merge2 = other.merge(indexAccessControl);
        assertTrue(merge2.getFieldPermissions().grantsAccessTo("a"));
        assertTrue(merge2.getFieldPermissions().grantsAccessTo("b"));
        assertTrue(merge2.getFieldPermissions().grantsAccessTo("c"));
        assertTrue(merge2.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge2.isGranted(), is(true));
        assertThat(merge2.getQueries(), nullValue());
    }

    public void testMergeEmptyAndNullFields() {
        IndexAccessControl indexAccessControl = new IndexAccessControl(true, new FieldPermissions(new String[]{}, null), null);
        IndexAccessControl other = new IndexAccessControl(true, new FieldPermissions(), null);

        IndexAccessControl merge1 = indexAccessControl.merge(other);
        assertFalse(merge1.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge1.isGranted(), is(true));
        assertThat(merge1.getQueries(), nullValue());

        IndexAccessControl merge2 = other.merge(indexAccessControl);
        assertFalse(merge1.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge2.isGranted(), is(true));
        assertThat(merge2.getQueries(), nullValue());
    }

    public void testMergeNullFields() {
        IndexAccessControl indexAccessControl = new IndexAccessControl(true, new FieldPermissions(new String[]{"a", "b"}, null), null);
        IndexAccessControl other = new IndexAccessControl(true, new FieldPermissions(), null);

        IndexAccessControl merge1 = indexAccessControl.merge(other);
        assertFalse(merge1.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge1.isGranted(), is(true));
        assertThat(merge1.getQueries(), nullValue());

        IndexAccessControl merge2 = other.merge(indexAccessControl);
        assertFalse(merge1.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge2.isGranted(), is(true));
        assertThat(merge2.getQueries(), nullValue());
    }

    public void testMergeQueries() {
        BytesReference query1 = new BytesArray(new byte[] { 0x1 });
        BytesReference query2 = new BytesArray(new byte[] { 0x2 });
        IndexAccessControl indexAccessControl = new IndexAccessControl(true, new FieldPermissions(), Collections.singleton
                (query1));
        IndexAccessControl other = new IndexAccessControl(true, new FieldPermissions(), Collections.singleton(query2));

        IndexAccessControl merge1 = indexAccessControl.merge(other);
        assertFalse(merge1.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge1.isGranted(), is(true));
        assertThat(merge1.getQueries(), containsInAnyOrder(query1, query2));

        IndexAccessControl merge2 = other.merge(indexAccessControl);
        assertFalse(merge1.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge2.isGranted(), is(true));
        assertThat(merge1.getQueries(), containsInAnyOrder(query1, query2));
    }

    public void testMergeNullQuery() {
        BytesReference query1 = new BytesArray(new byte[] { 0x1 });
        IndexAccessControl indexAccessControl = new IndexAccessControl(true, new FieldPermissions(), Collections.singleton
                (query1));
        IndexAccessControl other = new IndexAccessControl(true, new FieldPermissions(), null);

        IndexAccessControl merge1 = indexAccessControl.merge(other);
        assertFalse(merge1.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge1.isGranted(), is(true));
        assertThat(merge1.getQueries(), nullValue());

        IndexAccessControl merge2 = other.merge(indexAccessControl);
        assertFalse(merge1.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(merge2.isGranted(), is(true));
        assertThat(merge1.getQueries(), nullValue());
    }

    public void testMergeNotGrantedAndGranted() {
        final String[] notGrantedFields = randomFrom(new String[]{}, new String[]{"baz"}, null);
        final Set<BytesReference> notGrantedQueries = randomFrom(Collections.<BytesReference>emptySet(), null,
                Collections.<BytesReference>singleton(new BytesArray(new byte[] { randomByte() })));
        final IndexAccessControl indexAccessControl = new IndexAccessControl(false, new FieldPermissions(notGrantedFields, null),
                notGrantedQueries);

        final BytesReference query1 = new BytesArray(new byte[] { 0x1 });
        final String[] fields =
                randomFrom(new String[]{"foo"}, new String[]{"foo", "bar"}, new String[]{}, null);
        final Set<BytesReference> queries =
                randomFrom(Collections.singleton(query1), Collections.<BytesReference>emptySet(), null);
        final IndexAccessControl other = new IndexAccessControl(true, new FieldPermissions(fields, null), queries);

        IndexAccessControl merged = indexAccessControl.merge(other);
        assertThat(merged.isGranted(), is(true));
        assertThat(merged.getQueries(), equalTo(queries));
        if (fields == null) {
            assertFalse(merged.getFieldPermissions().hasFieldLevelSecurity());
        } else {
            assertTrue(merged.getFieldPermissions().hasFieldLevelSecurity());
            if (notGrantedFields != null) {
                for (String field : notGrantedFields) {
                    assertFalse(merged.getFieldPermissions().grantsAccessTo(field));
                }
            }
            for (String field : fields) {
                assertTrue(merged.getFieldPermissions().grantsAccessTo(field));
            }
        }
        merged = other.merge(indexAccessControl);
        assertThat(merged.isGranted(), is(true));
        assertThat(merged.getQueries(), equalTo(queries));
        if (fields == null) {
            assertFalse(merged.getFieldPermissions().hasFieldLevelSecurity());
        } else {
            assertTrue(merged.getFieldPermissions().hasFieldLevelSecurity());
            if (notGrantedFields != null) {
                for (String field : notGrantedFields) {
                    assertFalse(merged.getFieldPermissions().grantsAccessTo(field));
                }
            }
            for (String field : fields) {
                assertTrue(merged.getFieldPermissions().grantsAccessTo(field));
            }
        }
    }

    public void testMergeNotGranted() {
        final String[] notGrantedFields = randomFrom(new String[]{}, new String[]{"baz"}, null);
        final Set<BytesReference> notGrantedQueries = randomFrom(Collections.<BytesReference>emptySet(), null,
                Collections.<BytesReference>singleton(new BytesArray(new byte[] { randomByte() })));
        final IndexAccessControl indexAccessControl = new IndexAccessControl(false, new FieldPermissions(notGrantedFields, null),
                notGrantedQueries);

        final BytesReference query1 = new BytesArray(new byte[] { 0x1 });
        final String[] fields =
                randomFrom(new String[]{"foo"}, new String[]{"foo", "bar"}, new String[]{}, null);
        final Set<BytesReference> queries =
                randomFrom(Collections.singleton(query1), Collections.<BytesReference>emptySet(), null);
        final IndexAccessControl other = new IndexAccessControl(false, new FieldPermissions(fields, null), queries);

        IndexAccessControl merged = indexAccessControl.merge(other);
        assertThat(merged.isGranted(), is(false));
        assertThat(merged.getQueries(), equalTo(notGrantedQueries));

        merged = other.merge(indexAccessControl);
        assertThat(merged.isGranted(), is(false));
        assertThat(merged.getQueries(), equalTo(queries));
    }
}
