/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityIndexReaderWrapperUnitTests extends ESTestCase {

    private static final Set<String> META_FIELDS;
    static {
        final Set<String> metaFields = new HashSet<>(IndicesModule.getBuiltInMetadataFields());
        metaFields.add(SourceFieldMapper.NAME);
        metaFields.add(FieldNamesFieldMapper.NAME);
        metaFields.add(SeqNoFieldMapper.NAME);
        META_FIELDS = Collections.unmodifiableSet(metaFields);
    }

    private SecurityContext securityContext;
    private ScriptService scriptService;
    private SecurityIndexReaderWrapper securityIndexReaderWrapper;
    private ElasticsearchDirectoryReader esIn;
    private MockLicenseState licenseState;

    @Before
    public void setup() throws Exception {
        Index index = new Index("_index", "testUUID");
        scriptService = mock(ScriptService.class);

        ShardId shardId = new ShardId(index, 0);
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        securityContext = new SecurityContext(Settings.EMPTY, new ThreadContext(Settings.EMPTY));
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);

        Directory directory = new MMapDirectory(createTempDir());
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());
        writer.close();

        DirectoryReader in = DirectoryReader.open(directory); // unfortunately DirectoryReader isn't mock friendly
        esIn = ElasticsearchDirectoryReader.wrap(in, shardId);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        esIn.close();
    }

    public void testDefaultMetaFields() throws Exception {
        securityIndexReaderWrapper = new SecurityIndexReaderWrapper(null, null, securityContext, licenseState, scriptService) {
            @Override
            protected IndicesAccessControl getIndicesAccessControl() {
                IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(
                    new FieldPermissions(fieldPermissionDef(new String[] {}, null)),
                    DocumentPermissions.allowAll()
                );
                return new IndicesAccessControl(true, singletonMap("_index", indexAccessControl));
            }
        };

        FieldSubsetReader.FieldSubsetDirectoryReader result = (FieldSubsetReader.FieldSubsetDirectoryReader) securityIndexReaderWrapper
            .apply(esIn);
        assertThat(result.getFilter().run("_uid"), is(true));
        assertThat(result.getFilter().run("_id"), is(true));
        assertThat(result.getFilter().run("_version"), is(true));
        assertThat(result.getFilter().run("_type"), is(true));
        assertThat(result.getFilter().run("_source"), is(true));
        assertThat(result.getFilter().run("_routing"), is(true));
        assertThat(result.getFilter().run("_timestamp"), is(true));
        assertThat(result.getFilter().run("_ttl"), is(true));
        assertThat(result.getFilter().run("_size"), is(true));
        assertThat(result.getFilter().run("_index"), is(true));
        assertThat(result.getFilter().run("_field_names"), is(true));
        assertThat(result.getFilter().run("_seq_no"), is(true));
        assertThat(result.getFilter().run("_some_random_meta_field"), is(true));
        assertThat(result.getFilter().run("some_random_regular_field"), is(false));
    }

    public void testWrapReaderWhenFeatureDisabled() throws Exception {
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(false);
        securityIndexReaderWrapper = new SecurityIndexReaderWrapper(null, null, securityContext, licenseState, scriptService);
        DirectoryReader reader = securityIndexReaderWrapper.apply(esIn);
        assertThat(reader, sameInstance(esIn));
    }

    public void testWildcards() throws Exception {
        Set<String> expected = new HashSet<>(META_FIELDS);
        expected.add("field1_a");
        expected.add("field1_b");
        expected.add("field1_c");
        assertResolved(new FieldPermissions(fieldPermissionDef(new String[] { "field1*" }, null)), expected, "field", "field2");
    }

    public void testDotNotion() throws Exception {
        Set<String> expected = new HashSet<>(META_FIELDS);
        expected.add("foo.bar");
        assertResolved(new FieldPermissions(fieldPermissionDef(new String[] { "foo.bar" }, null)), expected, "foo", "foo.baz", "bar.foo");

        expected = new HashSet<>(META_FIELDS);
        expected.add("foo.bar");
        assertResolved(new FieldPermissions(fieldPermissionDef(new String[] { "foo.*" }, null)), expected, "foo", "bar");
    }

    private void assertResolved(FieldPermissions permissions, Set<String> expected, String... fieldsToTest) {
        for (String field : expected) {
            assertThat(field, permissions.grantsAccessTo(field), is(true));
        }
        for (String field : fieldsToTest) {
            assertThat(field, permissions.grantsAccessTo(field), is(expected.contains(field)));
        }
    }

    public void testFieldPermissionsWithFieldExceptions() throws Exception {
        securityIndexReaderWrapper = new SecurityIndexReaderWrapper(null, null, securityContext, licenseState, null);
        String[] grantedFields = new String[] {};
        String[] deniedFields;
        Set<String> expected = new HashSet<>(META_FIELDS);
        // Presence of fields in a role with an empty array implies access to no fields except the meta fields
        assertResolved(
            new FieldPermissions(fieldPermissionDef(grantedFields, randomBoolean() ? null : new String[] {})),
            expected,
            "foo",
            "bar"
        );

        // make sure meta fields cannot be denied access to
        deniedFields = META_FIELDS.toArray(new String[0]);
        assertResolved(
            new FieldPermissions(fieldPermissionDef(null, deniedFields)),
            new HashSet<>(Arrays.asList("foo", "bar", "_some_plugin_meta_field"))
        );

        // check we can add all fields with *
        grantedFields = new String[] { "*" };
        expected = new HashSet<>(META_FIELDS);
        expected.add("foo");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, randomBoolean() ? null : new String[] {})), expected);

        // same with null
        grantedFields = null;
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, randomBoolean() ? null : new String[] {})), expected);

        // check we remove only excluded fields
        grantedFields = new String[] { "*" };
        deniedFields = new String[] { "xfield" };
        expected = new HashSet<>(META_FIELDS);
        expected.add("foo");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "xfield");

        // same with null
        grantedFields = null;
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "xfield");

        // some other checks
        grantedFields = new String[] { "field*" };
        deniedFields = new String[] { "field1", "field2" };
        expected = new HashSet<>(META_FIELDS);
        expected.add("field3");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "field1", "field2");

        grantedFields = new String[] { "field1", "field2" };
        deniedFields = new String[] { "field2" };
        expected = new HashSet<>(META_FIELDS);
        expected.add("field1");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "field1", "field2");

        grantedFields = new String[] { "field*" };
        deniedFields = new String[] { "field2" };
        expected = new HashSet<>(META_FIELDS);
        expected.add("field1");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "field2");

        deniedFields = new String[] { "field*" };
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), META_FIELDS, "field1", "field2");

        // empty array for allowed fields always means no field is allowed
        grantedFields = new String[] {};
        deniedFields = new String[] {};
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), META_FIELDS, "field1", "field2");

        // make sure all field can be explicitly allowed
        grantedFields = new String[] { "*" };
        deniedFields = randomBoolean() ? null : new String[] {};
        expected = new HashSet<>(META_FIELDS);
        expected.add("field1");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected);
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }
}
