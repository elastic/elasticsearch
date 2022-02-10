package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.eql.EqlSamplingTestCase;

public class EqlSamplingIT extends EqlSamplingTestCase {

    public EqlSamplingIT(String query, String name, long[] eventIds, String[] joinKeys) {
        super(query, name, eventIds, joinKeys);
    }

}
