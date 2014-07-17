/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class ESUsersModuleTests extends ElasticsearchTestCase {

    private Path users;
    private Path usersRoles;

    @Before
    public void init() throws Exception {
        users = Paths.get(getClass().getResource("users").toURI());
        usersRoles = Paths.get(getClass().getResource("users_roles").toURI());
    }

    @Test
    public void test() throws Exception {
        Injector injector = Guice.createInjector(new TestModule(users, usersRoles), new ESUsersModule());
        ESUsersRealm realm = injector.getInstance(ESUsersRealm.class);
        assertThat(realm, notNullValue());
        assertThat(realm.userPasswdStore, notNullValue());
        assertThat(realm.userPasswdStore, instanceOf(FileUserPasswdStore.class));
        assertThat(realm.userRolesStore, notNullValue());
        assertThat(realm.userRolesStore, instanceOf(FileUserRolesStore.class));
    }

    public static class TestModule extends AbstractModule {

        final Path users;
        final Path usersRoles;

        public TestModule(Path users, Path usersRoles) {
            this.users = users;
            this.usersRoles = usersRoles;
        }

        @Override
        protected void configure() {
            Settings settings = ImmutableSettings.builder()
                    .put("shield.authc.esusers.file.users", users.toAbsolutePath())
                    .put("shield.authc.esusers.file.users_roles", usersRoles.toAbsolutePath())
                    .build();
            Environment env = new Environment(settings);
            bind(Settings.class).toInstance(settings);
            bind(Environment.class).toInstance(env);
            bind(ThreadPool.class).toInstance(new ThreadPool("test"));
            bind(ResourceWatcherService.class).asEagerSingleton();
        }
    }
}
