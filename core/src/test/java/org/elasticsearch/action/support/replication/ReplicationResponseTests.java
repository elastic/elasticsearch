package org.elasticsearch.action.support.replication;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class ReplicationResponseTests extends ESTestCase {

    public void testShardInfoToString() {
        final int total = 5;
        final int successful = randomIntBetween(1, total);
        final ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(total, successful);
        assertThat(
            shardInfo.toString(),
            equalTo(String.format(Locale.ROOT, "{\"_shards\":{\"total\":5,\"successful\":%d,\"failed\":0}}", successful)));
    }

}
