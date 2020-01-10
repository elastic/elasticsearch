package org.elasticsearch.persistent;

/**
 * A dynamic persistent tasks executor is notified of updates to params of tasks.
 * @param <T> Type of allocated task
 * @param <P> TYpe of params
 */
public abstract class DynamicPersistentTasksExecutor<T extends AllocatedPersistentTask, P extends PersistentTaskParams>
    extends PersistentTasksExecutor<P> {
    public DynamicPersistentTasksExecutor(String taskName, String executor) {
        super(taskName, executor);
    }

    /**
     * Handle incoming params update.
     * @param task the task for which params where updated
     * @param newParams the new params.
     */
    protected abstract void paramsUpdated(T task, P newParams);
}
