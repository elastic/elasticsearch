/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.admin.indices.migrate.TransportMigrateIndexAction;
import org.elasticsearch.action.admin.indices.migrate.TransportMigrateIndexAction.DocumentMigrater;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.TransportMainAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestMainAction;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class ActionModuleTests extends ESTestCase {
    public void testSetupActionsContainsKnownBuiltin() {
        assertThat(ActionModule.setupActions(emptyList()),
                hasEntry(MainAction.INSTANCE.name(), new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class)));
    }

    public void testPluginCantOverwriteBuiltinAction() {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class));
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, () -> ActionModule.setupActions(singletonList(dupsMainAction)));
        assertEquals("action for name [" + MainAction.NAME + "] already registered", e.getMessage());
    }

    public void testPluginCanRegisterAction() {
        class FakeRequest extends ActionRequest<FakeRequest> {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }
        class FakeTransportAction extends TransportAction<FakeRequest, ActionResponse> {
            protected FakeTransportAction(Settings settings, String actionName, ThreadPool threadPool, ActionFilters actionFilters,
                    IndexNameExpressionResolver indexNameExpressionResolver, TaskManager taskManager) {
                super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, taskManager);
            }

            @Override
            protected void doExecute(FakeRequest request, ActionListener<ActionResponse> listener) {
            }
        }
        class FakeAction extends GenericAction<FakeRequest, ActionResponse> {
            protected FakeAction() {
                super("fake");
            }

            @Override
            public ActionResponse newResponse() {
                return null;
            }
        }
        FakeAction action = new FakeAction();
        ActionPlugin registersFakeAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(action, FakeTransportAction.class));
            }
        };
        assertThat(ActionModule.setupActions(singletonList(registersFakeAction)),
                hasEntry("fake", new ActionHandler<>(action, FakeTransportAction.class)));
    }

    public void testSetupRestHandlerContainsKnownBuiltin() {
        assertThat(ActionModule.setupRestHandlers(emptyList()), hasItem(RestMainAction.class));
    }

    public void testPluginCantOverwriteBuiltinRestHandler() {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<Class<? extends RestHandler>> getRestHandlers() {
                return singletonList(RestMainAction.class);
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, () -> ActionModule.setupRestHandlers(singletonList(dupsMainAction)));
        assertEquals("can't register the same [rest_handler] more than once for [" + RestMainAction.class.getName() + "]", e.getMessage());
    }

    public void testPluginCanRegisterRestHandler() {
        class FakeHandler implements RestHandler {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
            }
        }
        ActionPlugin registersFakeHandler = new ActionPlugin() {
            @Override
            public List<Class<? extends RestHandler>> getRestHandlers() {
                return singletonList(FakeHandler.class);
            }
        };
        assertThat(ActionModule.setupRestHandlers(singletonList(registersFakeHandler)), hasItem(FakeHandler.class));
    }

    public void testDefaultDocumentMigrater() {
        assertThat(ActionModule.pickDocumentMigrater(emptyList()),
                instanceOf(TransportMigrateIndexAction.EmptyIndexDocumentMigrater.class));
    }

    public void testPluginCanRegisterDocumentMigrater() {
        TransportMigrateIndexAction.DocumentMigrater docMigrater = new TransportMigrateIndexAction.EmptyIndexDocumentMigrater();
        ActionPlugin registersDocumentMigrater = new ActionPlugin() {
            @Override
            public DocumentMigrater getDocumentMigrater() {
                return docMigrater;
            }
        };
        assertSame(docMigrater, ActionModule.pickDocumentMigrater(singletonList(registersDocumentMigrater)));
    }

    public void testTwoPluginsRegisteringDocumentMigraterFails() {
        TransportMigrateIndexAction.DocumentMigrater docMigrater = new TransportMigrateIndexAction.EmptyIndexDocumentMigrater();
        ActionPlugin registersDocumentMigrater = new ActionPlugin() {
            @Override
            public DocumentMigrater getDocumentMigrater() {
                return docMigrater;
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, () -> ActionModule.pickDocumentMigrater(Arrays.asList(
                registersDocumentMigrater, registersDocumentMigrater)));
        assertThat(e.getMessage(), containsString("More than one plugin provided an implemnetation for "
                    + "[TransportMigrateIndexAction.DocumentMigrater]"));
    }

}
