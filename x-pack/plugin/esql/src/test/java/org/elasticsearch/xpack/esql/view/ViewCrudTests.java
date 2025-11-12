/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.view.ViewTests.randomView;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ViewCrudTests extends AbstractViewTestCase {

    private TestViewsApi viewsApi;

    @Before
    public void setup() throws Exception {
        super.setUp();
        this.viewsApi = new TestViewsApi();
    }

    @After
    public void tearDown() throws Exception {
        for (String name : this.viewsApi.viewService.list()) {
            viewsApi.delete(name);
        }
        super.tearDown();
    }

    public void testCrud() throws Exception {
        View view = randomView(XContentType.JSON);
        String name = "my-view";

        AtomicReference<Exception> error = viewsApi.save(name, view);
        assertThat(error.get(), nullValue());
        assertView(viewsApi.get(name), name, view);

        viewsApi.delete(name);
        assertThat(viewsApi.get(name).size(), equalTo(0));
    }

    public void testList() throws Exception {
        for (int i = 0; i < 10; i++) {
            View view = randomView(XContentType.JSON);
            String name = "my-view-" + i;

            AtomicReference<Exception> error = viewsApi.save(name, view);
            assertThat(error.get(), nullValue());
            assertView(viewsApi.get(name), name, view);
            assertThat(viewsApi.get().size(), equalTo(1 + i));
        }
        for (int i = 0; i < 10; i++) {
            String name = "my-view-" + i;
            assertThat(viewsApi.get(name).size(), equalTo(1));
            viewsApi.delete(name);
            expectThrows(name, IllegalArgumentException.class, equalTo("Views do not exist: " + name), () -> viewsApi.get(name));
            assertThat(viewsApi.get().size(), equalTo(9 - i));
        }
    }

    public void testUpdate() throws Exception {
        View view = randomView(XContentType.JSON);
        String name = "my-view";

        AtomicReference<Exception> error = viewsApi.save(name, view);
        assertThat(error.get(), nullValue());

        view = randomView(XContentType.JSON);
        error = viewsApi.save(name, view);
        assertThat(error.get(), nullValue());
        assertView(viewsApi.get(name), name, view);

        viewsApi.delete(name);
        assertThat(viewsApi.get(name).size(), equalTo(0));
    }

    public void testPutValidation() throws Exception {
        View view = randomView(XContentType.JSON);

        {
            String nullOrEmptyName = randomBoolean() ? "" : null;
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save(nullOrEmptyName, view));
            assertThat(error.getMessage(), equalTo("name is missing or empty"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save("my-view", null));
            assertThat(error.getMessage(), equalTo("view is missing"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save("my#view", view));
            assertThat(error.getMessage(), equalTo("Invalid view name [my#view], must not contain '#'"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save("..", view));
            assertThat(error.getMessage(), equalTo("Invalid view name [..], must not be '.' or '..'"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.save("myView", view));
            assertThat(error.getMessage(), equalTo("Invalid view name [myView], must be lowercase"));
        }
        {
            View invalidView = new View("FROMMM abc");
            ParsingException error = expectThrows(ParsingException.class, () -> viewsApi.save("name", invalidView));
            assertThat(error.getMessage(), containsString("mismatched input 'FROMMM'"));
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
        String nullOrEmptyName = randomBoolean() ? "" : null;

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewsApi.get(nullOrEmptyName));
        assertThat(error.getMessage(), equalTo("name is missing or empty"));
        assertThat(viewsApi.get("null-view").size(), equalTo(0));
    }

    public void testListValidation() throws Exception {
        Map<String, View> result = viewsApi.get("null-view");
        assertTrue(result.isEmpty());
    }

    private void assertView(Map<String, View> result, String name, View view) {
        assertThat(result.size(), equalTo(1));
        View found = result.get(name);
        assertThat(found, not(nullValue()));
        assertThat(found, equalTo(view));
    }
}
