/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.stats.MemoryUsageTests;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AnalyticsResultTests extends AbstractXContentTestCase<AnalyticsResult> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected AnalyticsResult createTestInstance() {
        RowResults rowResults = null;
        Integer progressPercent = null;
        TrainedModelDefinition.Builder inferenceModel = null;
        if (randomBoolean()) {
            rowResults = RowResultsTests.createRandom();
        }
        if (randomBoolean()) {
            progressPercent = randomIntBetween(0, 100);
        }
        if (randomBoolean()) {
            inferenceModel = TrainedModelDefinitionTests.createRandomBuilder();
        }
        return new AnalyticsResult(rowResults, progressPercent, inferenceModel, MemoryUsageTests.createRandom());
    }

    @Override
    protected AnalyticsResult doParseInstance(XContentParser parser) {
        return AnalyticsResult.PARSER.apply(parser, null);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
