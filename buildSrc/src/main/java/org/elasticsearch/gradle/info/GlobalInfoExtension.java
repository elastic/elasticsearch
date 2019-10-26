package org.elasticsearch.gradle.info;

import java.util.ArrayList;
import java.util.List;

public class GlobalInfoExtension {
    final List<Runnable> listeners = new ArrayList<>();

    public void ready(Runnable block) {
        listeners.add(block);
    }
}
