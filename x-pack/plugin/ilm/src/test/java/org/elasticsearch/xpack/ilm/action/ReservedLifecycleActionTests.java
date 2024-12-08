/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReservedLifecycleActionTests extends ESTestCase {

    public void testActionNamedXContentRegistry() throws Exception {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        XPackLicenseState xPackLicenseState = mock(XPackLicenseState.class);
        String json = """
            {
              "policy": {
                "phases": {
                  "warm": {
                    "min_age": "10s",
                    "actions": {
                      "readonly" : { }
                    }
                  }
                }
              }
            }
            """;

        {
            // action has all named x-content from ILM plugin
            ReservedLifecycleAction action = new ReservedLifecycleAction(
                new NamedXContentRegistry(IndexLifecycle.NAMED_X_CONTENT_ENTRIES),
                client,
                xPackLicenseState
            );
            List<LifecyclePolicy> policies;
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
                policies = action.fromXContent(parser);
            }
            assertThat(policies.get(0).getPhases().get("warm").getActions().get("readonly"), instanceOf(ReadOnlyAction.class));
        }

        {
            // action is missing named x-content entry for readonly parser
            ReservedLifecycleAction action = new ReservedLifecycleAction(
                new NamedXContentRegistry(
                    IndexLifecycle.NAMED_X_CONTENT_ENTRIES.stream()
                        .filter(Predicate.not(entry -> entry.name.getPreferredName().equals(ReadOnlyAction.NAME)))
                        .toList()
                ),
                client,
                xPackLicenseState
            );

            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
                var exception = expectThrows(XContentParseException.class, () -> action.fromXContent(parser));
                assertThat(exception.getMessage(), containsString("[lifecycle_policy] failed to parse field [phases]"));
            }
        }
    }
}
