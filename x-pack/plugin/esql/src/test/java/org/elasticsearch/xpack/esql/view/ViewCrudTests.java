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

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.view.ViewTests.randomView;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ViewCrudTests extends AbstractViewTestCase {

    private ViewService viewService;

    @Before
    public void setup() throws Exception {
        super.setUp();
        this.viewService = viewService();
    }

    @After
    public void tearDown() throws Exception {
        for (String name : this.viewService.list()) {
            deleteView(name, viewService);
        }
        super.tearDown();
    }

    public void testCrud() throws Exception {
        ViewService viewService = viewService();
        View view = randomView(XContentType.JSON);
        String name = "my-view";

        AtomicReference<Exception> error = saveView(name, view, viewService);
        assertThat(error.get(), nullValue());

        View result = viewService.get(name);
        assertThat(result, equalTo(view));

        Set<String> views = viewService.list();
        assertThat(views.size(), equalTo(1));
        assertThat(views, equalTo(Set.of(name)));

        deleteView(name, viewService);
        result = viewService.get(name);
        assertThat(result, nullValue());
    }

    public void testUpdate() throws Exception {
        ViewService viewService = viewService();
        View view = randomView(XContentType.JSON);
        String name = "my-view";

        AtomicReference<Exception> error = saveView(name, view, viewService);
        assertThat(error.get(), nullValue());

        view = randomView(XContentType.JSON);
        error = saveView(name, view, viewService);
        assertThat(error.get(), nullValue());
        View result = viewService.get(name);
        assertThat(result, equalTo(view));

        deleteView(name, viewService);
        result = viewService.get(name);
        assertThat(result, nullValue());
    }

    public void testPutValidation() throws Exception {
        ViewService viewService = viewService();
        View view = randomView(XContentType.JSON);

        {
            String nullOrEmptyName = randomBoolean() ? "" : null;

            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> saveView(nullOrEmptyName, view, viewService)
            );

            assertThat(error.getMessage(), equalTo("name is missing or empty"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> saveView("my-view", null, viewService));

            assertThat(error.getMessage(), equalTo("view is missing"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> saveView("my#view", view, viewService));
            assertThat(error.getMessage(), equalTo("Invalid view name [my#view], must not contain '#'"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> saveView("..", view, viewService));
            assertThat(error.getMessage(), equalTo("Invalid view name [..], must not be '.' or '..'"));
        }
        {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> saveView("myView", view, viewService));
            assertThat(error.getMessage(), equalTo("Invalid view name [myView], must be lowercase"));
        }
        {
            View invalidView = new View("FROMMM abc");
            ParsingException error = expectThrows(ParsingException.class, () -> saveView("name", invalidView, viewService));
            assertThat(error.getMessage(), containsString("mismatched input 'FROMMM'"));
        }
    }

    public void testDeleteValidation() {
        ViewService viewService = viewService();

        {
            String nullOrEmptyName = randomBoolean() ? "" : null;

            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> deleteView(nullOrEmptyName, viewService));

            assertThat(error.getMessage(), equalTo("name is missing or empty"));
        }
        {
            ResourceNotFoundException error = expectThrows(ResourceNotFoundException.class, () -> deleteView("my-view", viewService));

            assertThat(error.getMessage(), equalTo("view [my-view] not found"));
        }
    }

    public void testGetValidation() {
        ViewService viewService = viewService();
        String nullOrEmptyName = randomBoolean() ? "" : null;

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> viewService.get(nullOrEmptyName));

        assertThat(error.getMessage(), equalTo("name is missing or empty"));

        View view = viewService.get("null-view");
        assertNull(view);
    }

    public void testListValidation() {
        ViewService viewService = viewService();
        Set<String> views = viewService.list();
        assertTrue(views.isEmpty());
    }

}
