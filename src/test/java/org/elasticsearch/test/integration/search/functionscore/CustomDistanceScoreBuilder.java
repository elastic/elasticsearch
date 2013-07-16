package org.elasticsearch.test.integration.search.functionscore;

import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;

public class CustomDistanceScoreBuilder extends DecayFunctionBuilder {


    @Override
    public String getName() {
        return CustomDistanceScoreParser.NAMES[0];
    }

}
