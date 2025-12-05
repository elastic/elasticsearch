/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomName;
import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomView;
import static org.elasticsearch.xpack.esql.plugin.EsqlFeatures.ESQL_VIEWS_FEATURE_FLAG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@LuceneTestCase.AwaitsFix(bugUrl = "to be resolved")
public class ViewCrudTests extends AbstractViewTestCase {

    private TestViewsApi viewsApi;

    @Rule
    // TODO: Remove this once we make ViewMetadata no longer snapshot-only
    public TestRule skipIfNotSnapshot = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeTrue("These tests only work in SNAPSHOT builds", ESQL_VIEWS_FEATURE_FLAG.isEnabled());
            base.evaluate();
        }
    };

    @Before
    public void setup() throws Exception {
        super.setUp();
        this.viewsApi = new TestViewsApi();
    }

    @After
    public void tearDown() throws Exception {
        for (String name : this.viewsApi.viewService.list(viewsApi.projectId)) {
            viewsApi.delete(name);
        }
        super.tearDown();
    }

    public void testCrud() throws Exception {
        String name = "my-view";
        View view = randomView(name);

        AtomicReference<Exception> error = viewsApi.save(view);
        assertThat(error.get(), nullValue());
        assertView(viewsApi.get(name), name, view);

        viewsApi.delete(name);
        assertViewMissing(viewsApi, name, 0);
    }

    public void testList() throws Exception {
        for (int i = 0; i < 10; i++) {
            String name = "my-view-" + i;
            View view = randomView(name);

            AtomicReference<Exception> error = viewsApi.save(view);
            assertThat(error.get(), nullValue());
            assertView(viewsApi.get(name), name, view);
            assertThat(viewsApi.get().size(), equalTo(1 + i));
        }
        for (int i = 0; i < 10; i++) {
            String name = "my-view-" + i;
            assertThat(viewsApi.get(name).size(), equalTo(1));
            viewsApi.delete(name);
            assertViewMissing(viewsApi, name, 9 - i);
        }
    }

    public void testUpdate() throws Exception {
        String name = "my-view";
        View view = randomView(name);

        AtomicReference<Exception> error = viewsApi.save(view);
        assertThat(error.get(), nullValue());

        view = randomView(name);
        error = viewsApi.save(view);
        assertThat(error.get(), nullValue());
        assertView(viewsApi.get(name), name, view);

        viewsApi.delete(name);
        assertViewMissing(viewsApi, name, 0);
    }

    public void testPutValidation() throws Exception {
        View view = randomView(randomName());

        {
            String nullOrEmptyName = randomBoolean() ? "" : null;
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save(randomView(nullOrEmptyName)));
            assertThat(error.getMessage(), equalTo("name is missing or empty"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save(new View("my-view", null)));
            assertThat(error.getMessage(), equalTo("view query is missing or empty"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save(randomView("my#view")));
            assertThat(error.getMessage(), equalTo("Invalid view name [my#view], must not contain '#'"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save(randomView("..")));
            assertThat(error.getMessage(), equalTo("Invalid view name [..], must not be '.' or '..'"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save(randomView("myView")));
            assertThat(error.getMessage(), equalTo("Invalid view name [myView], must be lowercase"));
        }
        {
            View invalidView = new View("name", "FROMMM abc");
            ParsingException error = expectThrows(ParsingException.class, () -> viewsApi.save(invalidView));
            assertThat(error.getMessage(), containsString("mismatched input 'FROMMM'"));
        }
        {
            View invalidView = new View("name", "FROM abc | SELECT 1 AS");
            ParsingException error = expectThrows(ParsingException.class, () -> viewsApi.save(invalidView));
            assertThat(error.getMessage(), containsString("mismatched input 'SELECT'"));
        }
    }

    public void testDeleteValidation() {
        {
            String nullOrEmptyName = randomBoolean() ? "" : null;
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.delete(nullOrEmptyName));
            assertThat(error.getMessage(), equalTo("name is missing or empty"));
        }
        {
            ResourceNotFoundException error = expectThrows(ResourceNotFoundException.class, () -> viewsApi.delete("my-view"));
            assertThat(error.getMessage(), equalTo("view [my-view] not found"));
        }
    }

    public void testGetValidation() throws Exception {
        expectThrows("null name", IllegalArgumentException.class, equalTo("name is missing or empty"), () -> viewsApi.get((String) null));
        expectThrows("empty name", IllegalArgumentException.class, equalTo("name is missing or empty"), () -> viewsApi.get(""));
        expectThrows("missing view", ResourceNotFoundException.class, equalTo("Views do not exist: name"), () -> viewsApi.get("name"));
        expectThrows(
            "missing views",
            ResourceNotFoundException.class,
            equalTo("Views do not exist: v1, v2"),
            () -> viewsApi.get("v1", "v2")
        );
        viewsApi.save(randomView("v2"));
        expectThrows(
            "partially missing views",
            ResourceNotFoundException.class,
            equalTo("Views do not exist: v1, v3"),
            () -> viewsApi.get("v1", "v2", "v3")
        );
    }

    private void assertView(List<View> result, String name, View view) {
        assertThat(result.size(), equalTo(1));
        Optional<View> found = result.stream().filter(v -> v.name().equals(name)).findFirst();
        assertFalse(found.isEmpty());
        assertThat(found.get(), equalTo(view));
    }

    private void assertViewMissing(TestViewsApi viewsApi, String name, int viewCount) throws Exception {
        expectThrows(name, ResourceNotFoundException.class, equalTo("Views do not exist: " + name), () -> viewsApi.get(name));
        assertThat(viewsApi.get().size(), equalTo(viewCount));
    }
}
