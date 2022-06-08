/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Updating cluster state in operator mode, for file based settings and modules/plugins, requires
 * that we have a separate update handler interface to the REST handlers. This interface declares
 * the basic contract for implementing cluster state update handlers in operator mode.
 */
public interface OperatorHandler<T extends MasterNodeRequest<?>> {
    String CONTENT = "content";

    /**
     * The operator handler name is a unique identifier that is matched to a section in a
     * cluster state update content. The operator cluster state updates are done as a single
     * cluster state update and the cluster state is typically supplied as a combined content,
     * unlike the REST handlers. This name must match a desired content key name in the combined
     * cluster state update, e.g. "ilm" or "cluster_settings" (for persistent cluster settings update).
     *
     * @return a String with the operator key name
     */
    String name();

    /**
     * The transform method of the operator handler should apply the necessary changes to
     * the cluster state as it normally would in a REST handler. One difference is that the
     * transform method in an operator handler must perform all CRUD operations of the cluster
     * state in one go. For that reason, we supply a wrapper class to the cluster state called
     * {@link TransformState}, which contains the current cluster state as well as any previous keys
     * set by this handler on prior invocation.
     *
     * @param source The parsed information specific to this handler from the combined cluster state content
     * @param prevState The previous cluster state and keys set by this handler (if any)
     * @return The modified state and the current keys set by this handler
     * @throws Exception
     */
    TransformState transform(Object source, TransformState prevState) throws Exception;

    /**
     * Sometimes certain parts of the cluster state cannot be created/updated without previously
     * setting other cluster state components, e.g. composable templates. Since the cluster state handlers
     * are processed in random order by the OperatorClusterStateController, this method gives an opportunity
     * to any operator handler to declare other operator handlers it depends on. Given dependencies exist,
     * the OperatorClusterStateController will order those handlers such that the handlers that are dependent
     * on are processed first.
     *
     * @return a collection of operator handler names
     */
    default Collection<String> dependencies() {
        return Collections.emptyList();
    }

    /**
     * All implementations of OperatorHandler should call the request validate method, by calling this default
     * implementation. To aid in any special validation logic that may need to be implemented by the operator handler
     * we provide this convenience method.
     *
     * @param request the master node request that we base this operator handler on
     */
    default void validate(T request) {
        ActionRequestValidationException exception = request.validate();
        if (exception != null) {
            throw new IllegalStateException("Validation error", exception);
        }
    }

    /**
     * Convenience method to convert the incoming passed in input to the transform method into a map.
     *
     * @param input the input passed into the operator handler after parsing the content
     * @return
     */
    @SuppressWarnings("unchecked")
    default Map<String, ?> asMap(Object input) {
        if (input instanceof Map<?, ?> source) {
            return (Map<String, Object>) source;
        }
        throw new IllegalStateException("Unsupported " + name() + " request format");
    }

    /**
     * Convenience method that creates a {@link XContentParser} from a content map so that it can be passed to
     * existing REST based code for input parsing.
     *
     * @param source the operator content as a map
     * @return
     */
    default XContentParser mapToXContentParser(Map<String, ?> source) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.map(source);
            return XContentFactory.xContent(builder.contentType())
                .createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder));
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }
}
