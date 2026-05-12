/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

record RemoteFetchResult(Page inputPage, int[] groupByPosition, int[] offsetByPosition, List<GroupPages> pagesByGroup) {
    static RemoteFetchResult passthrough(Page inputPage) {
        return new RemoteFetchResult(inputPage, null, null, null);
    }

    boolean isPassthrough() {
        return groupByPosition == null;
    }
}

record GroupPages(List<Page> pages, boolean hasPositionMapping) {}

/**
 * Fetches deferred fields from the owning data nodes after the coordinator has narrowed the candidate set.
 */
public final class RemoteFetchOperator extends AsyncOperator<RemoteFetchResult> {
    public interface Client extends Releasable {
        void fetchAsync(String nodeId, RemoteFetchService.Request request, ActionListener<List<Page>> listener);

        @Override
        default void close() {}
    }

    @FunctionalInterface
    public interface ClientFactory {
        Client create();
    }

    public record Factory(
        int handleChannel,
        List<RemoteFetchService.FetchField> requestFields,
        List<Attribute> outputFields,
        PhysicalPlan pushdownPlan,
        Configuration configuration,
        int maxOutstandingRequests,
        ThreadContext threadContext,
        ClientFactory clientFactory
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new RemoteFetchOperator(
                driverContext,
                threadContext,
                handleChannel,
                requestFields,
                outputFields,
                pushdownPlan,
                configuration,
                maxOutstandingRequests,
                clientFactory.create()
            );
        }

        @Override
        public String describe() {
            return "RemoteFetchOperator[channel=" + handleChannel + ", requestFields=" + requestFields + "]";
        }
    }

    private record TargetSession(String nodeId, String sessionId) {}

    private static final class Group {
        private final TargetSession target;
        private final List<RemoteFetchHandle> handles = new ArrayList<>();

        private Group(TargetSession target) {
            this.target = target;
        }
    }

    private final DriverContext driverContext;
    private final int handleChannel;
    private final List<RemoteFetchService.FetchField> requestFields;
    private final List<Attribute> outputFields;
    private final PhysicalPlan pushdownPlan;
    private final Configuration configuration;
    private final Client client;

    RemoteFetchOperator(
        DriverContext driverContext,
        ThreadContext threadContext,
        int handleChannel,
        List<RemoteFetchService.FetchField> requestFields,
        List<Attribute> outputFields,
        PhysicalPlan pushdownPlan,
        Configuration configuration,
        int maxOutstandingRequests,
        Client client
    ) {
        super(driverContext, threadContext, maxOutstandingRequests);
        this.driverContext = driverContext;
        this.handleChannel = handleChannel;
        this.requestFields = List.copyOf(requestFields);
        this.outputFields = List.copyOf(outputFields);
        this.pushdownPlan = pushdownPlan;
        this.configuration = configuration;
        this.client = client;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<RemoteFetchResult> listener) {
        if (inputPage.getPositionCount() == 0 || outputFields.isEmpty()) {
            listener.onResponse(RemoteFetchResult.passthrough(inputPage));
            return;
        }

        GroupedHandles groupedHandles = decodeHandles(inputPage);
        if (groupedHandles.groups().isEmpty()) {
            listener.onResponse(RemoteFetchResult.passthrough(inputPage));
            return;
        }

        inputPage.allowPassingToDifferentDriver();
        AtomicBoolean completed = new AtomicBoolean();
        AtomicInteger remaining = new AtomicInteger(groupedHandles.groups().size());
        List<GroupPages> pagesByGroup = new ArrayList<>(groupedHandles.groups().size());
        for (int i = 0; i < groupedHandles.groups().size(); i++) {
            pagesByGroup.add(null);
        }

        for (int groupIndex = 0; groupIndex < groupedHandles.groups().size(); groupIndex++) {
            Group group = groupedHandles.groups().get(groupIndex);
            RemoteFetchService.Request request = new RemoteFetchService.Request(
                group.target.sessionId(),
                requestFields,
                group.handles,
                pushdownPlan,
                configuration
            );
            final int currentGroup = groupIndex;
            client.fetchAsync(group.target.nodeId(), request, ActionListener.wrap(pages -> {
                pages.forEach(Page::allowPassingToDifferentDriver);
                if (completed.get()) {
                    releasePages(pages);
                    return;
                }
                final boolean hasPositionMapping;
                try {
                    hasPositionMapping = validateFetchedPages(group, pages);
                } catch (Exception e) {
                    releasePages(pages);
                    if (completed.compareAndSet(false, true)) {
                        releasePagesByGroup(pagesByGroup);
                        listener.onFailure(e);
                    }
                    return;
                }
                pagesByGroup.set(currentGroup, new GroupPages(pages, hasPositionMapping));
                if (remaining.decrementAndGet() == 0 && completed.compareAndSet(false, true)) {
                    listener.onResponse(
                        new RemoteFetchResult(inputPage, groupedHandles.groupByPosition(), groupedHandles.offsetByPosition(), pagesByGroup)
                    );
                }
            }, e -> {
                if (completed.compareAndSet(false, true)) {
                    releasePagesByGroup(pagesByGroup);
                    listener.onFailure(e);
                }
            }));
        }
    }

    @Override
    protected void releaseFetchedOnAnyThread(RemoteFetchResult result) {
        releasePageOnAnyThread(result.inputPage());
        releasePagesByGroup(result.pagesByGroup());
    }

    @Override
    protected void doClose() {
        client.close();
    }

    @Override
    public Page getOutput() {
        RemoteFetchResult fetched = fetchFromBuffer();
        if (fetched == null) {
            return null;
        }
        if (fetched.isPassthrough()) {
            return fetched.inputPage();
        }
        return mergeFetchedPage(fetched.inputPage(), fetched.groupByPosition(), fetched.offsetByPosition(), fetched.pagesByGroup());
    }

    @Override
    public String toString() {
        return "RemoteFetchOperator[channel=" + handleChannel + ", requestFields=" + requestFields + "]";
    }

    private GroupedHandles decodeHandles(Page inputPage) {
        BytesRefBlock handlesBlock = (BytesRefBlock) inputPage.getBlock(handleChannel);
        Map<TargetSession, Integer> groupLookup = new LinkedHashMap<>();
        List<Group> groups = new ArrayList<>();
        int[] groupByPosition = new int[inputPage.getPositionCount()];
        int[] offsetByPosition = new int[inputPage.getPositionCount()];
        BytesRef scratch = new BytesRef();

        for (int position = 0; position < inputPage.getPositionCount(); position++) {
            if (handlesBlock.isNull(position)) {
                throw new IllegalStateException("remote fetch handle column cannot contain nulls");
            }
            if (handlesBlock.getValueCount(position) != 1) {
                throw new IllegalStateException("remote fetch handle column must contain exactly one handle per row");
            }
            RemoteFetchHandle handle = RemoteFetchHandle.fromBytesRef(
                handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch)
            );
            TargetSession target = new TargetSession(handle.nodeId(), handle.sessionId());
            Integer groupIndex = groupLookup.get(target);
            if (groupIndex == null) {
                groupIndex = groups.size();
                groupLookup.put(target, groupIndex);
                groups.add(new Group(target));
            }
            Group group = groups.get(groupIndex);
            groupByPosition[position] = groupIndex;
            offsetByPosition[position] = group.handles.size();
            group.handles.add(handle);
        }
        return new GroupedHandles(groups, groupByPosition, offsetByPosition);
    }

    private boolean validateFetchedPages(Group group, List<Page> pages) {
        int positions = 0;
        Boolean hasPositionMapping = null;
        for (Page page : pages) {
            boolean pageHasPosition = page.getBlockCount() == outputFields.size() + 1;
            if (page.getBlockCount() != outputFields.size() && pageHasPosition == false) {
                throw new IllegalStateException(
                    "remote fetch returned ["
                        + page.getBlockCount()
                        + "] columns but expected ["
                        + outputFields.size()
                        + "] or ["
                        + (outputFields.size() + 1)
                        + "]"
                );
            }
            if (hasPositionMapping == null) {
                hasPositionMapping = pageHasPosition;
            } else if (hasPositionMapping != pageHasPosition) {
                throw new IllegalStateException("remote fetch returned inconsistent page schemas for a single target group");
            }
            positions += page.getPositionCount();
        }
        if (Boolean.TRUE.equals(hasPositionMapping) == false && positions != group.handles.size()) {
            throw new IllegalStateException("remote fetch returned [" + positions + "] rows but expected [" + group.handles.size() + "]");
        }
        return Boolean.TRUE.equals(hasPositionMapping);
    }

    private Page mergeFetchedPage(Page inputPage, int[] groupByPosition, int[] offsetByPosition, List<GroupPages> pagesByGroup) {
        if (pagesByGroup.stream().anyMatch(g -> g != null && g.hasPositionMapping())) {
            return mergeFetchedPageWithFiltering(inputPage, groupByPosition, offsetByPosition, pagesByGroup);
        }
        Block[] outputBlocks = new Block[inputPage.getBlockCount() + outputFields.size()];
        Block.Builder[] builders = new Block.Builder[outputFields.size()];
        boolean success = false;
        try {
            for (int block = 0; block < inputPage.getBlockCount(); block++) {
                outputBlocks[block] = inputPage.getBlock(block);
                outputBlocks[block].incRef();
            }
            for (int field = 0; field < outputFields.size(); field++) {
                builders[field] = PlannerUtils.toElementType(outputFields.get(field).dataType())
                    .newBlockBuilder(inputPage.getPositionCount(), driverContext.blockFactory());
                for (int position = 0; position < inputPage.getPositionCount(); position++) {
                    List<Page> fetchedPages = pagesByGroup.get(groupByPosition[position]).pages();
                    copyFetchedPosition(builders[field], fetchedPages, field, offsetByPosition[position]);
                }
                outputBlocks[inputPage.getBlockCount() + field] = builders[field].build();
            }
            Page output = new Page(inputPage.getPositionCount(), outputBlocks);
            success = true;
            return output;
        } finally {
            inputPage.releaseBlocks();
            releasePagesByGroup(pagesByGroup);
            Releasables.closeExpectNoException(Releasables.wrap(Arrays.asList(builders)));
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(Arrays.asList(outputBlocks)));
            }
        }
    }

    private Page mergeFetchedPageWithFiltering(
        Page inputPage,
        int[] groupByPosition,
        int[] offsetByPosition,
        List<GroupPages> pagesByGroup
    ) {
        List<Map<Integer, FetchedRowRef>> groupMappings = buildGroupMappings(pagesByGroup);
        List<Integer> originalPositions = new ArrayList<>(inputPage.getPositionCount());
        List<FetchedRowRef> keptRows = new ArrayList<>(inputPage.getPositionCount());
        for (int position = 0; position < inputPage.getPositionCount(); position++) {
            int group = groupByPosition[position];
            int offset = offsetByPosition[position];
            FetchedRowRef rowRef = groupMappings.get(group).get(offset);
            if (rowRef != null) {
                originalPositions.add(position);
                keptRows.add(rowRef);
            }
        }
        Block[] outputBlocks = new Block[inputPage.getBlockCount() + outputFields.size()];
        Block.Builder[] builders = new Block.Builder[outputBlocks.length];
        boolean success = false;
        try {
            for (int i = 0; i < inputPage.getBlockCount(); i++) {
                builders[i] = inputPage.getBlock(i).elementType().newBlockBuilder(originalPositions.size(), driverContext.blockFactory());
            }
            for (int i = 0; i < outputFields.size(); i++) {
                builders[inputPage.getBlockCount() + i] = PlannerUtils.toElementType(outputFields.get(i).dataType())
                    .newBlockBuilder(originalPositions.size(), driverContext.blockFactory());
            }
            for (int i = 0; i < originalPositions.size(); i++) {
                int inputPos = originalPositions.get(i);
                for (int block = 0; block < inputPage.getBlockCount(); block++) {
                    builders[block].copyFrom(inputPage.getBlock(block), inputPos, inputPos + 1);
                }
                FetchedRowRef rowRef = keptRows.get(i);
                Page fetchedPage = pagesByGroup.get(rowRef.group()).pages().get(rowRef.pageIndex());
                for (int field = 0; field < outputFields.size(); field++) {
                    builders[inputPage.getBlockCount() + field].copyFrom(
                        fetchedPage.getBlock(field),
                        rowRef.position(),
                        rowRef.position() + 1
                    );
                }
            }
            for (int i = 0; i < outputBlocks.length; i++) {
                outputBlocks[i] = builders[i].build();
            }
            Page output = new Page(originalPositions.size(), outputBlocks);
            success = true;
            return output;
        } finally {
            inputPage.releaseBlocks();
            releasePagesByGroup(pagesByGroup);
            Releasables.closeExpectNoException(Releasables.wrap(Arrays.asList(builders)));
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(Arrays.asList(outputBlocks)));
            }
        }
    }

    private static List<Map<Integer, FetchedRowRef>> buildGroupMappings(List<GroupPages> pagesByGroup) {
        List<Map<Integer, FetchedRowRef>> mappings = new ArrayList<>(pagesByGroup.size());
        for (int group = 0; group < pagesByGroup.size(); group++) {
            GroupPages groupPages = pagesByGroup.get(group);
            Map<Integer, FetchedRowRef> mapping = new HashMap<>();
            if (groupPages != null) {
                List<Page> pages = groupPages.pages();
                if (groupPages.hasPositionMapping()) {
                    for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
                        Page page = pages.get(pageIndex);
                        IntBlock positionBlock = page.getBlock(page.getBlockCount() - 1);
                        for (int row = 0; row < page.getPositionCount(); row++) {
                            mapping.put(positionBlock.getInt(row), new FetchedRowRef(group, pageIndex, row));
                        }
                    }
                } else {
                    int runningOffset = 0;
                    for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
                        Page page = pages.get(pageIndex);
                        for (int row = 0; row < page.getPositionCount(); row++) {
                            mapping.put(runningOffset++, new FetchedRowRef(group, pageIndex, row));
                        }
                    }
                }
            }
            mappings.add(mapping);
        }
        return mappings;
    }

    private static void copyFetchedPosition(Block.Builder builder, List<Page> fetchedPages, int fieldIndex, int flattenedPosition) {
        int position = flattenedPosition;
        for (Page page : fetchedPages) {
            if (position < page.getPositionCount()) {
                builder.copyFrom(page.getBlock(fieldIndex), position, position + 1);
                return;
            }
            position -= page.getPositionCount();
        }
        throw new IllegalStateException("remote fetch response did not contain the expected row");
    }

    private static void releasePagesByGroup(List<GroupPages> pagesByGroup) {
        for (GroupPages group : pagesByGroup) {
            releasePages(group == null ? null : group.pages());
        }
    }

    private static void releasePages(List<Page> pages) {
        if (pages != null) {
            for (Page page : pages) {
                Releasables.closeExpectNoException(page::releaseBlocks);
            }
        }
    }

    private record GroupedHandles(List<Group> groups, int[] groupByPosition, int[] offsetByPosition) {}

    private record FetchedRowRef(int group, int pageIndex, int position) {}
}
