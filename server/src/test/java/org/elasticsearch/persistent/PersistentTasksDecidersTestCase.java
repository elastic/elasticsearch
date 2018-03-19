package org.elasticsearch.persistent;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;

public abstract class PersistentTasksDecidersTestCase extends ESTestCase {

    /** Needed by {@link ClusterService} **/
    private static ThreadPool threadPool;
    /** Needed by {@link PersistentTasksClusterService} **/
    private ClusterService clusterService;

    private PersistentTasksClusterService persistentTasksClusterService;

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestClass().getSimpleName());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(clusterService.getSettings(), emptyList()) {
            @Override
            public <Params extends PersistentTaskParams> PersistentTasksExecutor<Params> getPersistentTaskExecutorSafe(String taskName) {
                return new PersistentTasksExecutor<Params>(clusterService.getSettings(), taskName, null) {
                    @Override
                    protected void nodeOperation(AllocatedPersistentTask task, Params params, Task.Status status) {
                        logger.debug("Executing task {}", task);
                    }
                };
            }
        };
        persistentTasksClusterService = new PersistentTasksClusterService(clusterService.getSettings(), registry, clusterService);
    }

    @AfterClass
    public static void tearDownThreadPool() throws Exception {
        terminate(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    protected ClusterState reassign(final ClusterState clusterState) {
        return persistentTasksClusterService.reassignTasks(clusterState);
    }

    protected static ClusterState createClusterStateWithTasks(final Settings settings, final int nbNodes, final int nbTasks) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < nbNodes; i++) {
            nodes.add(new DiscoveryNode("_node_" + i, buildNewFakeTransportAddress(), Version.CURRENT));
        }

        PersistentTasksCustomMetaData.Builder tasks = PersistentTasksCustomMetaData.builder();
        for (int i = 0; i < nbTasks; i++) {
            tasks.addTask("_task_" + i, "test", null, new PersistentTasksCustomMetaData.Assignment(null, "initialized"));
        }

        MetaData metaData = MetaData.builder()
            .putCustom(PersistentTasksCustomMetaData.TYPE, tasks.build())
            .persistentSettings(settings)
            .build();

        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metaData(metaData).build();
    }

    /** Asserts that the given cluster state contains nbTasks tasks that are assigned **/
    protected static void assertNbAssignedTasks(final long nbTasks, final ClusterState clusterState) {
        assertPersistentTasks(nbTasks, clusterState, PersistentTasksCustomMetaData.PersistentTask::isAssigned);
    }

    /** Asserts that the given cluster state contains nbTasks tasks that are NOT assigned **/
    protected static void assertNbUnassignedTasks(final long nbTasks, final ClusterState clusterState) {
        assertPersistentTasks(nbTasks, clusterState, task -> task.isAssigned() == false);
    }

    /** Asserts that the cluster state contains nbTasks tasks that verify the given predicate **/
    protected static void assertPersistentTasks(final long nbTasks,
                                              final ClusterState clusterState,
                                              final Predicate<PersistentTasksCustomMetaData.PersistentTask> predicate) {
        PersistentTasksCustomMetaData tasks = clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        assertNotNull("Persistent tasks must be not null", tasks);
        assertEquals(nbTasks, tasks.tasks().stream().filter(predicate).count());
    }
}
