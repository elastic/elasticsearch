/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import org.junit.Test;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

public class ExternalChangelogSourceTests {

    @Test
    public void testParseSourceRepoFromYaml() {
        String yaml = """
            pr: 3008
            summary: "Harden pytorch_inference with TorchScript model graph validation"
            area: Machine Learning
            type: enhancement
            issues:
              - 2890
            source_repo: elastic/ml-cpp
            """;

        ChangelogEntry entry = ChangelogEntry.parse(yaml);
        assertThat(entry.getSourceRepo(), equalTo("elastic/ml-cpp"));
        assertThat(entry.getRepoUrl(), equalTo("https://github.com/elastic/ml-cpp"));
        assertThat(entry.getPr(), equalTo(3008));
        assertThat(entry.getSummary(), equalTo("Harden pytorch_inference with TorchScript model graph validation"));
    }

    @Test
    public void testDefaultRepoUrlWhenSourceRepoAbsent() {
        String yaml = """
            pr: 145000
            summary: "Some ES change"
            area: Search
            type: enhancement
            """;

        ChangelogEntry entry = ChangelogEntry.parse(yaml);
        assertThat(entry.getSourceRepo(), nullValue());
        assertThat(entry.getRepoUrl(), equalTo("https://github.com/elastic/elasticsearch"));
    }

    @Test
    public void testResolveElasticsearchGitRef() {
        assertThat(BundleChangelogsTask.resolveElasticsearchGitRef("main", "origin"), equalTo("origin/main"));
        assertThat(BundleChangelogsTask.resolveElasticsearchGitRef("upstream/9.1", "origin"), equalTo("origin/9.1"));
        assertThat(BundleChangelogsTask.resolveElasticsearchGitRef("abc1234567890", "origin"), equalTo("abc1234567890"));
        assertThat(BundleChangelogsTask.resolveElasticsearchGitRef("feature/foo", "myremote"), equalTo("myremote/feature/foo"));
    }

    @Test
    public void testNormalizeBranchStripsPrefixes() {
        assertThat(BundleChangelogsTask.normalizeBranchForExternalFetch("main"), equalTo("main"));
        assertThat(BundleChangelogsTask.normalizeBranchForExternalFetch("9.3"), equalTo("9.3"));
        assertThat(BundleChangelogsTask.normalizeBranchForExternalFetch("upstream/main"), equalTo("main"));
        assertThat(BundleChangelogsTask.normalizeBranchForExternalFetch("origin/9.3"), equalTo("9.3"));
    }

    @Test
    public void testNormalizeBranchStripsConfiguredElasticsearchRemote() {
        assertThat(BundleChangelogsTask.normalizeBranchForExternalFetch("elastic/9.2", "elastic"), equalTo("9.2"));
        assertThat(BundleChangelogsTask.normalizeBranchForExternalFetch("myfork/feature/x", "myfork"), equalTo("feature/x"));
    }

    @Test
    public void testNormalizeBranchPreservesSlashesInBranchNames() {
        assertThat(BundleChangelogsTask.normalizeBranchForExternalFetch("feature/foo"), equalTo("feature/foo"));
        assertThat(BundleChangelogsTask.normalizeBranchForExternalFetch("refs/heads/main"), equalTo("refs/heads/main"));
        assertThat(BundleChangelogsTask.normalizeBranchForExternalFetch("release/9.4.0"), equalTo("release/9.4.0"));
    }

    @Test
    public void testNormalizeBranchRejectsSha() {
        assertThrows(
            IllegalArgumentException.class,
            () -> BundleChangelogsTask.normalizeBranchForExternalFetch("abc1234def5678")
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> BundleChangelogsTask.normalizeBranchForExternalFetch("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
        );
    }

    @Test
    public void testIsShaRef() {
        assertThat(BundleChangelogsTask.isShaRef("abc1234"), equalTo(true));
        assertThat(BundleChangelogsTask.isShaRef("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"), equalTo(true));
        assertThat(BundleChangelogsTask.isShaRef("ABC1234DEF5678"), equalTo(true));
        assertThat(BundleChangelogsTask.isShaRef("aBc1234"), equalTo(true));
        assertThat(BundleChangelogsTask.isShaRef("main"), equalTo(false));
        assertThat(BundleChangelogsTask.isShaRef("9.3"), equalTo(false));
        assertThat(BundleChangelogsTask.isShaRef("upstream/main"), equalTo(false));
        assertThat(BundleChangelogsTask.isShaRef("feature/foo"), equalTo(false));
    }

    @Test
    public void testParseStringSetSourceRepoManually() {
        String yaml = """
            pr: 100
            summary: "A change"
            area: Machine Learning
            type: bug
            """;

        ChangelogEntry entry = ChangelogEntry.parse(yaml);
        assertThat(entry.getSourceRepo(), nullValue());
        assertThat(entry.getRepoUrl(), equalTo("https://github.com/elastic/elasticsearch"));

        entry.setSourceRepo("elastic/ml-cpp");
        assertThat(entry.getSourceRepo(), equalTo("elastic/ml-cpp"));
        assertThat(entry.getRepoUrl(), equalTo("https://github.com/elastic/ml-cpp"));
    }

    @Test
    public void testParseStringInvalidYamlThrows() {
        assertThrows(UncheckedIOException.class, () -> ChangelogEntry.parse("not: [valid: yaml: for: changelog"));
    }

    @Test
    public void testIsShaRefEdgeCases() {
        assertThat("6 chars (too short)", BundleChangelogsTask.isShaRef("abc123"), equalTo(false));
        assertThat("7 chars (minimum)", BundleChangelogsTask.isShaRef("abc1234"), equalTo(true));
        assertThat("40 chars (full SHA)", BundleChangelogsTask.isShaRef("a".repeat(40)), equalTo(true));
        assertThat("41 chars (too long)", BundleChangelogsTask.isShaRef("a".repeat(41)), equalTo(false));
        assertThat("contains non-hex", BundleChangelogsTask.isShaRef("abc123g"), equalTo(false));
        assertThat("empty string", BundleChangelogsTask.isShaRef(""), equalTo(false));
    }

    @Test
    public void testExternalChangelogSourceAccessors() {
        var source = new BundleChangelogsTask.ExternalChangelogSource(
            "https://github.com/elastic/ml-cpp.git",
            "elastic/ml-cpp",
            "docs/changelog"
        );

        assertThat(source.repoUrl(), equalTo("https://github.com/elastic/ml-cpp.git"));
        assertThat(source.sourceRepo(), equalTo("elastic/ml-cpp"));
        assertThat(source.changelogPath(), equalTo("docs/changelog"));
    }

    @Test
    public void testChangelogEntryComparatorUsesSourceRepoForDuplicatePrNumbers() {
        var mlCpp = new ChangelogEntry();
        mlCpp.setPr(100);
        mlCpp.setSourceRepo("elastic/ml-cpp");
        var es = new ChangelogEntry();
        es.setPr(100);
        es.setSourceRepo(null);
        var other = new ChangelogEntry();
        other.setPr(100);
        other.setSourceRepo("elastic/other");

        var entries = new ArrayList<>(List.of(mlCpp, es, other));
        entries.sort(BundleChangelogsTask.changelogEntryComparator());
        assertThat(entries.get(0).getSourceRepo(), nullValue());
        assertThat(entries.get(1).getSourceRepo(), equalTo("elastic/ml-cpp"));
        assertThat(entries.get(2).getSourceRepo(), equalTo("elastic/other"));
    }
}
