/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class SnapshotUtilsTests extends ESTestCase {
    public void testIndexNameFiltering() {
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] {}, new String[] { "foo", "bar", "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "*" }, new String[] { "foo", "bar", "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "_all" }, new String[] { "foo", "bar", "baz" });
        assertIndexNameFiltering(
            new String[] { "foo", "bar", "baz" },
            new String[] { "foo", "bar", "baz" },
            new String[] { "foo", "bar", "baz" }
        );
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "foo" }, new String[] { "foo" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "baz", "not_available" }, new String[] { "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "ba*", "-bar", "-baz" }, new String[] {});
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "-bar" }, new String[] { "foo", "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "-ba*" }, new String[] { "foo" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "+ba*" }, new String[] { "bar", "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "+bar", "+foo" }, new String[] { "bar", "foo" });
        assertIndexNameFiltering(
            new String[] { "foo", "bar", "baz" },
            new String[] { "zzz", "bar" },
            IndicesOptions.lenientExpandOpen(),
            new String[] { "bar" }
        );
        assertIndexNameFiltering(
            new String[] { "foo", "bar", "baz" },
            new String[] { "" },
            IndicesOptions.lenientExpandOpen(),
            new String[] {}
        );
        assertIndexNameFiltering(
            new String[] { "foo", "bar", "baz" },
            new String[] { "foo", "", "ba*" },
            IndicesOptions.lenientExpandOpen(),
            new String[] { "foo", "bar", "baz" }
        );
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, String[] expected) {
        assertIndexNameFiltering(indices, filter, IndicesOptions.lenientExpandOpen(), expected);
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, IndicesOptions indicesOptions, String[] expected) {
        List<String> indicesList = Arrays.asList(indices);
        List<String> actual = SnapshotUtils.filterIndices(indicesList, filter, indicesOptions);
        assertThat(actual, containsInAnyOrder(expected));
    }
}
