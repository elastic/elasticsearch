/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class FilteredSearchExecutionContextTests extends ESTestCase {
    public void testCopyForConcurrentUse() {
        SearchExecutionContext innerContext = SearchExecutionContextTests.createSearchExecutionContext("uuid", null);
        FilteredSearchExecutionContext filteredContext = new FilteredSearchExecutionContext(innerContext);

        FilteredSearchExecutionContext clone = filteredContext.copyForConcurrentUse();
        assertThat(filteredContext, is(not((sameInstance(clone)))));
        // The copy should have a different SearchExecutionContext at it's root
        assertThat(clone.getInnerContext(), is(not((sameInstance(innerContext)))));
    }

    public void testCloneForConcurrentUsePreservesOverriddenMethods() {
        SearchExecutionContext innerContext = SearchExecutionContextTests.createSearchExecutionContext("uuid", null);
        // override a method
        FilteredSearchExecutionContext anonymousClass = new FilteredSearchExecutionContext(innerContext) {
            @Override
            public List<String> defaultFields() {
                return List.of("this is the anonymous filtered context");
            }
        };

        FilteredSearchExecutionContext clone = anonymousClass.copyForConcurrentUse();
        // test that the cloned instance preserves the anonymous class override
        assertThat(clone, is(not((sameInstance(anonymousClass)))));
        assertThat(clone.defaultFields(), is(List.of("this is the anonymous filtered context")));
    }

    public void testCopyForConcurrentUseWithDeeplyNestedFilteredContexts() {
        SearchExecutionContext coreContext = SearchExecutionContextTests.createSearchExecutionContext("uuid", null);
        FilteredSearchExecutionContext innerContext = new FilteredSearchExecutionContext(coreContext) {
            @Override
            public List<String> defaultFields() {
                return List.of("this is the anonymous filtered context");
            }
        };
        FilteredSearchExecutionContext sandwichFillingContext = new FilteredSearchExecutionContext(innerContext);
        FilteredSearchExecutionContext outerContext = new FilteredSearchExecutionContext(sandwichFillingContext);
        // Check outer delegates to inner
        assertThat(outerContext.defaultFields(), is(List.of("this is the anonymous filtered context")));

        FilteredSearchExecutionContext clone = outerContext.copyForConcurrentUse();
        assertThat(outerContext, is(not((sameInstance(clone)))));
        assertThat(clone.defaultFields(), is(List.of("this is the anonymous filtered context")));

        FilteredSearchExecutionContext unwrappedSandwichFillingContext = (FilteredSearchExecutionContext) clone.getInnerContext();
        assertThat(unwrappedSandwichFillingContext, is(not((sameInstance(sandwichFillingContext)))));
        FilteredSearchExecutionContext unwrappedInnerContext = (FilteredSearchExecutionContext) unwrappedSandwichFillingContext
            .getInnerContext();
        assertThat(unwrappedInnerContext, is(not((sameInstance(innerContext)))));

        // The copy should have a different SearchExecutionContext at the root
        assertThat(unwrappedInnerContext.getInnerContext(), is(not((sameInstance(coreContext)))));

        // check modifying the original's prefiltering scope does not change the clone's
        outerContext.autoPrefilteringScope().push(List.of(mock(QueryBuilder.class)));
        assertThat(clone.autoPrefilteringScope().getPrefilters(), is(empty()));
    }
}
