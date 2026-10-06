/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.physical.DocRefEncodeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.remotefetch.RemoteFetchHandle;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataNodeComputeHandlerTests extends ESTestCase {

    public void testMalformedRemoteFetchBoundaryFinishesOpenSinkBeforeComputeStarts() {
        // This path fails before compute starts, so transport/search collaborators are inert mocks; a real ExchangeService verifies
        // that the already-open sink is actually removed rather than merely checking a mocked invocation.
        ComputeService computeService = mock(ComputeService.class);
        PlannerSettings.Holder plannerSettings = mock(PlannerSettings.Holder.class);
        when(computeService.plannerSettings()).thenReturn(plannerSettings);
        when(plannerSettings.get()).thenReturn(PlannerSettings.DEFAULTS);
        when(computeService.createFlags()).thenReturn(new EsqlFlags(false));

        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.localNode()).thenReturn(localNode);
        when(localNode.getId()).thenReturn("node-a");

        TransportService transportService = mock(TransportService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.relativeTimeInMillisSupplier()).thenReturn(System::currentTimeMillis);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        ExchangeService exchangeService = new ExchangeService(
            Settings.EMPTY,
            threadPool,
            ThreadPool.Names.SEARCH,
            TestBlockFactory.getNonBreakingInstance()
        );
        Executor directExecutor = Runnable::run;
        DataNodeComputeHandler handler = new DataNodeComputeHandler(
            computeService,
            clusterService,
            mock(ProjectResolver.class),
            mock(SearchService.class),
            transportService,
            exchangeService,
            directExecutor
        );

        DataNodeRequest request = malformedRemoteFetchRequest("session-a");
        exchangeService.createSinkHandler(request.sessionId(), 1);
        TransportChannel channel = mock(TransportChannel.class);
        when(channel.getVersion()).thenReturn(TransportVersion.current());

        handler.messageReceived(request, channel, mock(Task.class));

        expectThrows(ResourceNotFoundException.class, () -> exchangeService.getSinkHandler(request.sessionId()));
    }

    /**
     * Rows that leave a data node as document references name a context the fetch phase finds again. A request without
     * fetch contexts would name contexts that close with the request.
     */
    public void testDocumentReferencesNeedFetchContexts() {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        ReferenceAttribute docRef = new ReferenceAttribute(
            Source.EMPTY,
            null,
            "$$doc_ref",
            DataType.DOC_REF,
            Nullability.FALSE,
            null,
            true
        );
        DocRefEncodeExec encode = new DocRefEncodeExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(doc), false),
            doc,
            docRef
        );
        ExchangeSinkExec plan = new ExchangeSinkExec(Source.EMPTY, encode.output(), false, encode);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DataNodeComputeHandler.validateFetchContexts(plan, request(plan, false, null))
        );
        assertThat(e.getMessage(), equalTo("document references need fetch contexts, but the request asked for none"));
        DataNodeComputeHandler.validateFetchContexts(plan, request(plan, false, TimeValue.timeValueMinutes(1)));
    }

    /**
     * The remote fetch prototype and the fetch phase each keep contexts open their own way, so a request asks for one.
     */
    public void testRetainedAndFetchContextsExcludeEachOther() {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        ExchangeSinkExec plan = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(doc),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(doc), false)
        );

        DataNodeComputeHandler.validateFetchContexts(plan, request(plan, false, TimeValue.timeValueMinutes(1)));
        DataNodeComputeHandler.validateFetchContexts(plan, request(plan, true, null));
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DataNodeComputeHandler.validateFetchContexts(plan, request(plan, true, TimeValue.timeValueMinutes(1)))
        );
        assertThat(e.getMessage(), equalTo("a request can't retain search contexts and open fetch contexts at once"));
    }

    private static DataNodeRequest request(ExchangeSinkExec plan, boolean retainSearchContexts, TimeValue fetchContextKeepAlive) {
        return new DataNodeRequest(
            "session",
            EsqlTestUtils.TEST_CFG,
            "",
            List.of(),
            Map.of(),
            plan,
            new String[0],
            IndicesOptions.STRICT_EXPAND_OPEN,
            true,
            true,
            retainSearchContexts,
            false,
            List.of(),
            fetchContextKeepAlive
        );
    }

    private static DataNodeRequest malformedRemoteFetchRequest(String sessionId) {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        Attribute handle = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchHandle.ATTRIBUTE_NAME,
            DataType.KEYWORD,
            Nullability.FALSE,
            null,
            true
        );
        RemoteFetchBoundaryExec boundary = new RemoteFetchBoundaryExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(doc), false),
            doc,
            handle,
            List.of()
        );
        ExchangeSinkExec sink = new ExchangeSinkExec(Source.EMPTY, boundary.handoffOutput(), false, boundary);
        return new DataNodeRequest(
            sessionId,
            EsqlTestUtils.TEST_CFG,
            "",
            List.of(),
            Map.of(),
            sink,
            new String[0],
            IndicesOptions.STRICT_EXPAND_OPEN,
            true,
            true,
            true,
            randomBoolean()
        );
    }
}
