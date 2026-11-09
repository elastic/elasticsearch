/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.spill.ResultPages;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

/**
 * Results from running a chunk of ESQL.
 *
 * @param schema               "Schema" of the {@link Attribute}s that are produced by the {@link LogicalPlan}
 *                             that was run. Each {@link Page} contains a {@link Block} of values for each
 *                             attribute in this list.
 * @param pages                Actual values produced by running the ESQL.
 * @param attributeMetadata    attribute metadata to return with response
 * @param configuration        The configuration used during the execution of this query.
 * @param completionInfo       Information collected from drivers after they've been completed.
 * @param executionInfo        Metadata about the execution of this query. Used for cross cluster queries.
 * @param approximationApplied Tri-state: null when query approximation was not requested (field omitted from the response),
 *                             otherwise whether approximation was actually applied for the query.
 */
public record Result(
    List<Attribute> schema,
    List<Page> pages,
    @Nullable ResultPages resultPages,
    Map<NameId, Map<String, Object>> attributeMetadata,
    Configuration configuration,
    DriverCompletionInfo completionInfo,
    @Nullable EsqlExecutionInfo executionInfo,
    @Nullable Boolean approximationApplied
) {
    /**
     * A result whose pages are the list: one that stayed in memory, a sub-plan's, a streamed query's, or a test's.
     */
    public Result(
        List<Attribute> schema,
        List<Page> pages,
        Map<NameId, Map<String, Object>> attributeMetadata,
        Configuration configuration,
        DriverCompletionInfo completionInfo,
        @Nullable EsqlExecutionInfo executionInfo,
        @Nullable Boolean approximationApplied
    ) {
        this(schema, pages, null, attributeMetadata, configuration, completionInfo, executionInfo, approximationApplied);
    }

    /**
     * @param resultPages the client's answer in a run, as the collector handed it over; null for a result whose pages
     *                    are the list, which {@link #toResultPages()} then wraps
     */
    public Result {}

    /**
     * The pages of a result that holds them as a list; a result whose pages went to a run throws, so that no path
     * drops the run for an empty list: such a result is read through {@link #toResultPages()}.
     */
    @Override
    public List<Page> pages() {
        if (resultPages != null) {
            throw new IllegalStateException("the result's pages are a source; read them through toResultPages()");
        }
        return pages;
    }

    /**
     * The list as it is, for passing the result on with other metadata.
     */
    public List<Page> residentPages() {
        return pages;
    }

    /**
     * The result's pages as the response holds them: the run when there is one, else the list.
     */
    public ResultPages toResultPages() {
        return resultPages != null ? resultPages : ResultPages.resident(pages);
    }

    /**
     * Releases the result's pages, on a path that will not hand them to a response.
     */
    public void releasePages() {
        if (resultPages != null) {
            resultPages.close();
        } else {
            Releasables.closeExpectNoException(pages);
        }
    }

    @Nullable
    public Map<String, Object> attributeMetadata(NameId name) {
        return attributeMetadata.get(name);
    }
}
