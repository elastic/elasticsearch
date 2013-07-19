package org.elasticsearch.test.integration.threadpool;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class SimpleThreadPoolTests extends AbstractNodesTests {
    
    private Client client1;

    private Client client2;

    private ThreadPool threadPool;

    @Override
    protected void beforeClass() {
        startNode("node1", ImmutableSettings.settingsBuilder().put("threadpool.search.type", "cached").build());
        startNode("node2", ImmutableSettings.settingsBuilder().put("threadpool.search.type", "cached").build());
        client1 = client("node1");
        client2 = client("node2");
        threadPool = ((InternalNode) node("node1")).injector().getInstance(ThreadPool.class);
    }

    @Test(timeout = 20000)
    public void testUpdatingThreadPoolSettings() throws Exception {
        // Check that settings are changed
        assertThat(((ThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(5L));
        client1.admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put("threadpool.search.keep_alive", "10m").build()).execute().actionGet();
        assertThat(((ThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));

        // Make sure that threads continue executing when executor is replaced
        final CyclicBarrier barrier = new CyclicBarrier(2);
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.executor(Names.SEARCH).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } catch (BrokenBarrierException ex) {
                    //
                }
            }
        });
        client1.admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put("threadpool.search.type", "fixed").build()).execute().actionGet();
        assertThat(threadPool.executor(Names.SEARCH), not(sameInstance(oldExecutor)));
        assertThat(((ThreadPoolExecutor) oldExecutor).isShutdown(), equalTo(true));
        assertThat(((ThreadPoolExecutor) oldExecutor).isTerminating(), equalTo(true));
        assertThat(((ThreadPoolExecutor) oldExecutor).isTerminated(), equalTo(false));
        barrier.await();

        // Make sure that new thread executor is functional
        threadPool.executor(Names.SEARCH).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } catch (BrokenBarrierException ex) {
                    //
                }
            }
        });
        client1.admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put("threadpool.search.type", "fixed").build()).execute().actionGet();
        barrier.await();
        Thread.sleep(200);

        // Check that node info is correct
        NodesInfoResponse nodesInfoResponse = client2.admin().cluster().prepareNodesInfo().all().execute().actionGet();
        for (int i = 0; i < 2; i++) {
            NodeInfo nodeInfo = nodesInfoResponse.getNodes()[i];
            boolean found = false;
            for (ThreadPool.Info info : nodeInfo.getThreadPool()) {
                if (info.getName().equals(Names.SEARCH)) {
                    assertThat(info.getType(), equalTo("fixed"));
                    assertThat(info.getRejectSetting(), equalTo("abort"));
                    assertThat(info.getQueueType(), equalTo("linked"));
                    found = true;
                    break;
                }
            }
            assertThat(found, equalTo(true));

            Map<String, Object> poolMap = getPoolSettingsThroughJson(nodeInfo.getThreadPool(), Names.SEARCH);
            assertThat(poolMap.get("reject_policy").toString(), equalTo("abort"));
            assertThat(poolMap.get("queue_type").toString(), equalTo("linked"));
        }

        client1.admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder()
                .put("threadpool.search.type", "blocking")
                .put("threadpool.search.wait_time", "10s")
                .put("threadpool.search.keep_alive", "15s")
                .put("threadpool.search.capacity", "100")
                .build()).execute().actionGet();
        Thread.sleep(200);
        nodesInfoResponse = client2.admin().cluster().prepareNodesInfo().all().execute().actionGet();
        for (int i = 0; i < 2; i++) {
            NodeInfo nodeInfo = nodesInfoResponse.getNodes()[i];
            boolean found = false;
            for (ThreadPool.Info info : nodeInfo.getThreadPool()) {
                if (info.getName().equals(Names.SEARCH)) {
                    assertThat(info.getType(), equalTo("blocking"));
                    assertThat(info.getQueueSize().singles(), equalTo(100L));
                    assertThat(info.getWaitTime().seconds(), equalTo(10L));
                    assertThat(info.getKeepAlive().seconds(), equalTo(15L));
                    found = true;
                    break;
                }
            }
            assertThat(found, equalTo(true));

            Map<String, Object> poolMap = getPoolSettingsThroughJson(nodeInfo.getThreadPool(), Names.SEARCH);
            assertThat(poolMap.get("queue_size").toString(), equalTo("100"));
            assertThat(poolMap.get("wait_time").toString(), equalTo("10s"));
            assertThat(poolMap.get("keep_alive").toString(), equalTo("15s"));
        }

    }

    private Map<String, Object> getPoolSettingsThroughJson(ThreadPoolInfo info, String poolName) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        info.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.close();
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.string());
        Map<String, Object> poolsMap = parser.mapAndClose();
        return (Map<String, Object>) ((Map<String, Object>) poolsMap.get("thread_pool")).get(poolName);
    }

}
