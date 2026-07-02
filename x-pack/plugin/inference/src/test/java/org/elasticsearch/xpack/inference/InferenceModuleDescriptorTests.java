/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.test.ESTestCase;

import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;

import static org.elasticsearch.module.BasicServerModuleTests.urlsToPaths;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.requiresOf;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.hasItem;

public class InferenceModuleDescriptorTests extends ESTestCase {

    private static final String INFERENCE_MODULE_NAME = "org.elasticsearch.inference";

    /**
     * Pins the {@code requires} directives that exist only to satisfy a transitive runtime
     * dependency that the inference plugin's own bytecode never references directly.
     *
     * <p>The dependency chain is:
     * <pre>
     *   CustomRequest
     *     -> ValidatingSubstitutor
     *          -> org.apache.commons.text.StringSubstitutor  (commons-text, an automatic module)
     *               -> org.apache.commons.lang3.*            (loaded internally by StringSubstitutor)
     * </pre>
     *
     * <p>{@code commons-text} is an <em>automatic</em> module (no {@code module-info.class}, only
     * an {@code Automatic-Module-Name} manifest entry). Automatic modules carry no {@code requires}
     * directives and therefore cannot pull any explicit module into the module graph on their own.
     * {@code commons-lang3} is an <em>explicit</em> module (ships a {@code module-info.class}),
     * so the "automatic module reads all other automatic modules" rule does not apply to it.
     *
     * <p>Consequently, without an explicit {@code requires org.apache.commons.lang3} in
     * {@code module-info.java}, the lang3 module is never added to the inference plugin's module
     * layer by {@code PluginsLoader.createModuleLayer}. Module layer resolution still succeeds
     * (no error at resolve time), but the first call that causes {@code StringSubstitutor} to load
     * a lang3 class throws a {@code NoClassDefFoundError} inside the running node.
     *
     * <p>This kind of failure is invisible to classpath-based unit tests (where JPMS {@code requires}
     * is never enforced) and only manifests when the plugin is loaded as a named module inside a
     * real Elasticsearch node — for example, during the validation inference call triggered by a
     * {@code PUT _inference} request for the Custom service.
     */
    public void testRequiresCommonsTextAndLang3() {
        var moduleDescriptor = getInferenceDescriptor();
        assertThat(moduleDescriptor.requires(), hasItem(requiresOf("org.apache.commons.text")));
    }

    private static ModuleDescriptor getInferenceDescriptor() {
        var finder = ModuleFinder.of(urlsToPaths(JarHell.parseClassPath()));
        var moduleReference = finder.find(INFERENCE_MODULE_NAME);
        assertThat(moduleReference, isPresent());
        return moduleReference.get().descriptor();
    }
}
