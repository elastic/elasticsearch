/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class StatelessComponentsTests extends ESTestCase {

    public void testStartStopAndCloseComponents() {
        final LifecycleComponent c1 = newComponent();
        final LifecycleComponent c2 = newComponent();

        StatelessComponents.startComponents(List.of(c1, c2));
        assertThat(c1.lifecycleState(), is(Lifecycle.State.STARTED));
        assertThat(c2.lifecycleState(), is(Lifecycle.State.STARTED));

        StatelessComponents.stopComponents(List.of(c2, c1));
        assertThat(c2.lifecycleState(), is(Lifecycle.State.STOPPED));
        assertThat(c1.lifecycleState(), is(Lifecycle.State.STOPPED));

        StatelessComponents.closeComponents(List.of(c2, c1));
        assertThat(c2.lifecycleState(), is(Lifecycle.State.CLOSED));
        assertThat(c1.lifecycleState(), is(Lifecycle.State.CLOSED));
    }

    public void testStartComponentsWithFaults() {
        final LifecycleComponent c1 = newComponent();
        final LifecycleComponent c2 = newComponentWithFaultyClose("c2");
        final LifecycleComponent c3 = newComponentWithFaultyStart("c3");

        try {
            StatelessComponents.startComponents(List.of(c1, c2, c3));
            fail("should have thrown exception");
        } catch (Exception e) {
            assertThat(c1.lifecycleState(), is(Lifecycle.State.CLOSED));
            assertThat(c2.lifecycleState(), is(Lifecycle.State.CLOSED));
            assertThat(c3.lifecycleState(), is(Lifecycle.State.INITIALIZED));
            assertThat(e.getMessage(), containsString("[c3] failed to start"));
            assertThat(e.getSuppressed(), arrayWithSize(1));
            assertThat(e.getSuppressed()[0].getMessage(), containsString("[c2] failed to close"));
        }
    }

    public void testCloseComponentsWithFaults() {
        final LifecycleComponent c1 = newComponentWithFaultyClose("c1");
        final LifecycleComponent c2 = newComponent();

        try {
            StatelessComponents.closeComponents(List.of(c1, c2));
            fail("should have thrown exception");
        } catch (Exception e) {
            assertThat(c1.lifecycleState(), is(Lifecycle.State.CLOSED));
            assertThat(c2.lifecycleState(), is(Lifecycle.State.CLOSED));
            assertThat(e.getMessage(), containsString("[c1] failed to close"));
        }
    }

    private LifecycleComponent newComponent() {
        return new AbstractLifecycleComponent() {

            @Override
            protected void doStart() {}

            @Override
            protected void doStop() {}

            @Override
            protected void doClose() throws IOException {}
        };
    }

    private LifecycleComponent newComponentWithFaultyStart(String componentName) {
        return new AbstractLifecycleComponent() {

            @Override
            protected void doStart() {
                throw new RuntimeException("[" + componentName + "] failed to start");
            }

            @Override
            protected void doStop() {}

            @Override
            protected void doClose() throws IOException {}
        };
    }

    private LifecycleComponent newComponentWithFaultyClose(String componentName) {
        return new AbstractLifecycleComponent() {

            @Override
            protected void doStart() {}

            @Override
            protected void doStop() {}

            @Override
            protected void doClose() throws IOException {
                throw new RuntimeException("[" + componentName + "] failed to close");
            }
        };
    }
}
