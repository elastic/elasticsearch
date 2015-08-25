package org.elasticsearch.devtools;

import org.gradle.api.*;

class RandomizedTesting implements Plugin<Project> {

  void apply(Project project) {
    project.task([type: RandomizedTestingRunner.class], 'test2')
  }
}
