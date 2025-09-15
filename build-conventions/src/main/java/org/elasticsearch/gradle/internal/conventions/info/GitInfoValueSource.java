package org.elasticsearch.gradle.internal.conventions.info;

import org.gradle.api.provider.Property;
import org.gradle.api.provider.ValueSource;
import org.gradle.api.provider.ValueSourceParameters;
import org.jetbrains.annotations.Nullable;

import java.io.File;

public abstract class GitInfoValueSource implements ValueSource<GitInfo, GitInfoValueSource.Parameters> {

    @Nullable
    @Override
    public GitInfo obtain() {
        File path = getParameters().getPath().get();
        return GitInfo.gitInfo(path);
    }

    public interface Parameters extends ValueSourceParameters {
        Property<File> getPath();
    }
}
