/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.session.schema.ResolvedSchema;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The federation showcase. Proves the architectural claim of the schema-discovery design: <b>resolving the schema of an
 * abstraction that lives on a remote cluster is literally "invoke that cluster's {@code SchemaService} umbrella"</b> —
 * the coordinator calls {@code resolve_schema} on the remote via the remote-cluster client, the remote runs its own
 * resolution against its own cluster state, and the resolved schema crosses the wire (real {@code writeTo}, not
 * {@code localOnly()}).
 *
 * <p>The abstraction used is a <b>view</b> (the §4 case): a view's definition lives in the remote's cluster state. The
 * remote's {@code resolve_schema} handler authorizes the view name against its own state ({@code resolveViews} filter)
 * and returns it tagged with the VIEW kind; the coordinator receives it as a {@link ResolvedSchema.Remote}. There is no
 * view of this name on the local cluster, so a success can only come from the remote umbrella having resolved it.
 *
 * <p>POC scope: this proves remote resolution through the umbrella action + the cross-cluster wire form + the kind tag.
 * The view's full output schema (its body analyzed against the remote's state) and cross-version protocol-compat (the
 * {@code #cps-project-team} "many protocol versions" problem) are production follow-ups.
 */
public class FederatedSchemaResolutionIT extends AbstractCrossClusterTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    /**
     * Create a view on the remote cluster, then resolve its schema by invoking {@code resolve_schema} on that remote
     * through the local cluster's remote-cluster client. The response — carried over the wire — must contain the view
     * with the VIEW kind tag, proving the remote umbrella resolved it from its own cluster state.
     */
    public void testRemoteViewSchemaResolvedThroughUmbrellaAction() throws Exception {
        // A backing index + a view over it, published on the REMOTE cluster only (via cluster-state CRUD).
        populateIndex(REMOTE_CLUSTER_1, "remote_logs", 1, 5);
        assertAcked(
            client(REMOTE_CLUSTER_1).execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TIMEOUT, TIMEOUT, new View("remote_view", "FROM remote_logs"))
            )
        );

        // Invoke resolve_schema on the remote via the remote-cluster client — the recursion to the remote umbrella.
        RemoteClusterService remoteClusterService = cluster(LOCAL_CLUSTER).getInstance(TransportService.class).getRemoteClusterService();
        var remoteClient = remoteClusterService.getRemoteClusterClient(
            REMOTE_CLUSTER_1,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
        );

        var request = new TransportResolveSchemaAction.Request(new String[] { "remote_view" });

        PlainActionFuture<EsqlResolveSchemaAction.Response> future = new PlainActionFuture<>();
        remoteClient.execute(TransportResolveSchemaAction.REMOTE_TYPE, request, future);
        EsqlResolveSchemaAction.Response response = future.actionGet(TIMEOUT);

        List<ResolvedSchema> schemas = response.schemas();
        assertThat("remote umbrella resolved exactly the requested view", schemas, hasSize(1));
        ResolvedSchema schema = schemas.get(0);
        // The wire form deserializes to the Remote carrier (real writeTo, not localOnly()).
        assertThat(schema, instanceOf(ResolvedSchema.Remote.class));
        ResolvedSchema.Remote remote = (ResolvedSchema.Remote) schema;
        assertThat(remote.name(), equalTo("remote_view"));
        assertThat(remote.kind(), equalTo(ResolvedSchema.Remote.Kind.VIEW));
    }
}
