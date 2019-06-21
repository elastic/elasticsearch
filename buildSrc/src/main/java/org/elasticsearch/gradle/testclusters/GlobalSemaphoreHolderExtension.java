package org.elasticsearch.gradle.testclusters;

import org.gradle.api.Project;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Semaphore;

public class GlobalSemaphoreHolderExtension {

    private final Semaphore globalSemaphore;

    public GlobalSemaphoreHolderExtension(Semaphore globalSemaphore) {
        this.globalSemaphore = globalSemaphore;
    }

    public static Semaphore get(Project project) {
        GlobalSemaphoreHolderExtension ext = project.getRootProject().getExtensions().findByType(GlobalSemaphoreHolderExtension.class);
        if (ext == null) {
           ext = project.getRootProject().getExtensions().create(
               "__globalSemaphore"  + UUID.randomUUID().toString(),
               GlobalSemaphoreHolderExtension.class,
               new Semaphore(maxPermits())
               );
        }
        return ext.globalSemaphore;
    }

    public static int maxPermits() {
        return Optional.ofNullable(System.getProperty("testclusters.max-nodes"))
            .map(Integer::valueOf)
            .orElse(Runtime.getRuntime().availableProcessors() / 2);
    }
}
