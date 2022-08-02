/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.hamcrest.Matchers.containsString;

public class FieldPermissionsTests extends ESTestCase {

    public void testParseFieldPermissions() throws Exception {
        String q = """
            {
              "indices": [
                {
                  "names": "idx2",
                  "privileges": [ "p3" ],
                  "field_security": {
                    "grant": [ "f1", "f2", "f3", "f4" ],
                    "except": [ "f3", "f4" ]
                  }
                }
              ]
            }""";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getGrantedFields(), new String[] { "f1", "f2", "f3", "f4" });
        assertArrayEquals(rd.getIndicesPrivileges()[0].getDeniedFields(), new String[] { "f3", "f4" });

        q = """
            {
              "indices": [
                {
                  "names": "idx2",
                  "privileges": [ "p3" ],
                  "field_security": {
                    "except": [ "f3", "f4" ],
                    "grant": [ "f1", "f2", "f3", "f4" ]
                  }
                }
              ]
            }""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getGrantedFields(), new String[] { "f1", "f2", "f3", "f4" });
        assertArrayEquals(rd.getIndicesPrivileges()[0].getDeniedFields(), new String[] { "f3", "f4" });

        q = """
            {
              "indices": [
                {
                  "names": "idx2",
                  "privileges": [ "p3" ],
                  "field_security": {
                    "grant": [ "f1", "f2" ]
                  }
                }
              ]
            }""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getGrantedFields(), new String[] { "f1", "f2" });
        assertNull(rd.getIndicesPrivileges()[0].getDeniedFields());

        q = """
            {
              "indices": [
                {
                  "names": "idx2",
                  "privileges": [ "p3" ],
                  "field_security": {
                    "grant": []
                  }
                }
              ]
            }""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getGrantedFields(), new String[] {});
        assertNull(rd.getIndicesPrivileges()[0].getDeniedFields());

        q = """
            {
              "indices": [
                {
                  "names": "idx2",
                  "privileges": [ "p3" ],
                  "field_security": {
                    "except": [],
                    "grant": []
                  }
                }
              ]
            }""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getGrantedFields(), new String[] {});
        assertArrayEquals(rd.getIndicesPrivileges()[0].getDeniedFields(), new String[] {});

        final String exceptWithoutGrant = """
            {
              "indices": [
                {
                  "names": "idx2",
                  "privileges": [ "p3" ],
                  "field_security": {
                    "except": [ "f1" ]
                  }
                }
              ]
            }""";
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(exceptWithoutGrant), false, XContentType.JSON)
        );
        assertThat(
            e.getDetailedMessage(),
            containsString("failed to parse indices privileges for role [test]. field_security" + " requires grant if except is given")
        );

        final String grantNull = """
            {"indices": [ {"names": "idx2", "privileges": ["p3"], "field_security": {"grant": null}}]}""";
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(grantNull), false, XContentType.JSON)
        );
        assertThat(
            e.getDetailedMessage(),
            containsString("failed to parse indices privileges for" + " role [test]. grant must not be null.")
        );

        final String exceptNull = """
            {"indices": [ {"names": "idx2", "privileges": ["p3"], "field_security": {"grant": ["*"],"except": null}}]}""";
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(exceptNull), false, XContentType.JSON)
        );
        assertThat(
            e.getDetailedMessage(),
            containsString("failed to parse indices privileges for role [test]. except must" + " not be null.")
        );

        final String exceptGrantNull = """
            {"indices": [ {"names": "idx2", "privileges": ["p3"], "field_security": {"grant": null,"except": null}}]}""";
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(exceptGrantNull), false, XContentType.JSON)
        );
        assertThat(
            e.getDetailedMessage(),
            containsString("failed to parse indices privileges " + "for role [test]. grant must not be null.")
        );

        final String bothFieldsMissing = """
            {"indices": [ {"names": "idx2", "privileges": ["p3"], "field_security": {}}]}""";
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(bothFieldsMissing), false, XContentType.JSON)
        );
        assertThat(
            e.getDetailedMessage(),
            containsString("failed to parse indices privileges " + "for role [test]. \"field_security\" must not be empty.")
        );

        // try with two indices and mix order a little
        q = """
            {
              "indices": [
                {
                  "names": "idx2",
                  "privileges": [ "p3" ],
                  "field_security": {
                    "grant": []
                  }
                },
                {
                  "names": "idx3",
                  "field_security": {
                    "grant": [ "*" ],
                    "except": [ "f2" ]
                  },
                  "privileges": [ "p3" ]
                }
              ]
            }""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getGrantedFields(), new String[] {});
        assertNull(rd.getIndicesPrivileges()[0].getDeniedFields());
        assertArrayEquals(rd.getIndicesPrivileges()[1].getGrantedFields(), new String[] { "*" });
        assertArrayEquals(rd.getIndicesPrivileges()[1].getDeniedFields(), new String[] { "f2" });
    }

    // test old syntax for field permissions
    public void testBWCFieldPermissions() throws Exception {
        String q = """
            {"indices": [ {"names": "idx2", "privileges": ["p3"], "fields": ["f1", "f2"]}]}""";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(q), true, XContentType.JSON);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getGrantedFields(), new String[] { "f1", "f2" });
        assertNull(rd.getIndicesPrivileges()[0].getDeniedFields());

        final String failingQuery = q;
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(failingQuery), false, XContentType.JSON)
        );
        assertThat(e.getDetailedMessage(), containsString("""
            ["fields": [...]] format has changed for field permissions in role [test], \
            use ["field_security": {"grant":[...],"except":[...]}] instead"""));

        q = """
            {"indices": [ {"names": "idx2", "privileges": ["p3"], "fields": []}]}""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), true, XContentType.JSON);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getGrantedFields(), new String[] {});
        assertNull(rd.getIndicesPrivileges()[0].getDeniedFields());
        final String failingQuery2 = q;
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(failingQuery2), false, XContentType.JSON)
        );
        assertThat(e.getDetailedMessage(), containsString("""
            ["fields": [...]] format has changed for field permissions in role [test], \
            use ["field_security": {"grant":[...],"except":[...]}] instead"""));

        q = """
            {"indices": [ {"names": "idx2", "privileges": ["p3"], "fields": null}]}""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), true, XContentType.JSON);
        assertNull(rd.getIndicesPrivileges()[0].getGrantedFields());
        assertNull(rd.getIndicesPrivileges()[0].getDeniedFields());
        final String failingQuery3 = q;
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(failingQuery3), false, XContentType.JSON)
        );
        assertThat(e.getDetailedMessage(), containsString("""
            ["fields": [...]] format has changed for field permissions in role [test], \
            use ["field_security": {"grant":[...],"except":[...]}] instead"""));
    }

    public void testFieldPermissionsHashCodeThreadSafe() throws Exception {
        final int numThreads = scaledRandomIntBetween(4, 16);
        final FieldPermissions fieldPermissions = new FieldPermissions(
            new FieldPermissionsDefinition(new String[] { "*" }, new String[] { "foo" })
        );
        final CountDownLatch latch = new CountDownLatch(numThreads + 1);
        final AtomicReferenceArray<Integer> hashCodes = new AtomicReferenceArray<>(numThreads);
        List<Thread> threads = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final int threadNum = i;
            threads.add(new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                final int hashCode = fieldPermissions.hashCode();
                hashCodes.set(threadNum, hashCode);
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        final int hashCode = fieldPermissions.hashCode();
        for (int i = 0; i < numThreads; i++) {
            assertEquals((Integer) hashCode, hashCodes.get(i));
        }
    }
}
