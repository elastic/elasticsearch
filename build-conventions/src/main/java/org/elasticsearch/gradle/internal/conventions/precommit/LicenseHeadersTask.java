/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions.precommit;

import org.apache.rat.Defaults;
import org.apache.rat.ReportConfiguration;
import org.apache.rat.analysis.IHeaderMatcher;
import org.apache.rat.analysis.util.HeaderMatcherMultiplexer;
import org.apache.rat.anttasks.SubstringLicenseMatcher;
import org.apache.rat.api.RatException;
import org.apache.rat.document.impl.FileDocument;
import org.apache.rat.license.SimpleLicenseFamily;
import org.apache.rat.report.RatReport;
import org.apache.rat.report.claim.ClaimStatistic;
import org.apache.rat.report.xml.XmlReportFactory;
import org.apache.rat.report.xml.writer.impl.base.XmlWriter;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Checks files for license headers..
 */
@CacheableTask
public abstract class LicenseHeadersTask extends DefaultTask {
    public LicenseHeadersTask() {
        setDescription("Checks sources for missing, incorrect, or unacceptable license headers");
    }

    /**
     * The list of java files to check. protected so the afterEvaluate closure in the
     * constructor can write to it.
     */
    @InputFiles
    @SkipWhenEmpty
    @PathSensitive(PathSensitivity.RELATIVE)
    public List<FileCollection> getJavaFiles() {
        return getSourceFolders().get();
    }

    @Internal
    public abstract ListProperty<FileCollection> getSourceFolders();

    public File getReportFile() {
        return reportFile;
    }

    public void setReportFile(File reportFile) {
        this.reportFile = reportFile;
    }

    public List<String> getApprovedLicenses() {
        return approvedLicenses;
    }

    public void setApprovedLicenses(List<String> approvedLicenses) {
        this.approvedLicenses = approvedLicenses;
    }

    public List<String> getExcludes() {
        return excludes;
    }

    public Map<String, String> getAdditionalLicenses() {
        return additionalLicenses;
    }

    public void setExcludes(List<String> excludes) {
        this.excludes = excludes;
    }

    @OutputFile
    private File reportFile = new File(getProject().getBuildDir(), "reports/licenseHeaders/rat.xml");

    /**
     * Allowed license families for this project.
     */
    @Input
    private List<String> approvedLicenses = new ArrayList<String>(Arrays.asList("SSPL+Elastic License", "Generated", "Vendored"));
    /**
     * Files that should be excluded from the license header check. Use with extreme care, only in situations where the license on the
     * source file is compatible with the codebase but we do not want to add the license to the list of approved headers (to avoid the
     * possibility of inadvertently using the license on our own source files).
     */
    @Input
    private List<String> excludes = new ArrayList<String>();
    /**
     * Additional license families that may be found. The key is the license category name (5 characters),
     * followed by the family name and the value list of patterns to search for.
     */
    @Input
    protected Map<String, String> additionalLicenses = new HashMap<String, String>();

    /**
     * Add a new license type.
     * <p>
     * The license may be added to the {@link #approvedLicenses} using the {@code familyName}.
     *
     * @param categoryName A 5-character string identifier for the license
     * @param familyName   An expanded string name for the license
     * @param pattern      A pattern to search for, which if found, indicates a file contains the license
     */
    public void additionalLicense(final String categoryName, String familyName, String pattern) {
        if (categoryName.length() != 5) {
            throw new IllegalArgumentException("License category name must be exactly 5 characters, got " + categoryName);
        }

        additionalLicenses.put(categoryName + familyName, pattern);
    }

    @TaskAction
    public void runRat() {
        ReportConfiguration reportConfiguration = new ReportConfiguration();
        reportConfiguration.setAddingLicenses(true);
        List<IHeaderMatcher> matchers = new ArrayList<>();
        matchers.add(Defaults.createDefaultMatcher());

        // BSD 4-clause stuff (is disallowed below)
        // we keep this here, in case someone adds BSD code for some reason, it should never be allowed.
        matchers.add(subStringMatcher("BSD4 ", "Original BSD License (with advertising clause)", "All advertising materials"));
        // Apache
        matchers.add(subStringMatcher("AL   ", "Apache", "Licensed to Elasticsearch B.V. under one or more contributor"));
        // Generated resources
        matchers.add(subStringMatcher("GEN  ", "Generated", "ANTLR GENERATED CODE"));
        // Vendored Code
        matchers.add(subStringMatcher("VEN  ", "Vendored", "@notice"));
        // Dual SSPLv1 and Elastic
        matchers.add(subStringMatcher("DUAL", "SSPL+Elastic License", "the Elastic License 2.0 or the Server"));

        for (Map.Entry<String, String> additional : additionalLicenses.entrySet()) {
            String category = additional.getKey().substring(0, 5);
            String family = additional.getKey().substring(5);
            matchers.add(subStringMatcher(category, family, additional.getValue()));
        }

        reportConfiguration.setHeaderMatcher(new HeaderMatcherMultiplexer(matchers.toArray(IHeaderMatcher[]::new)));
        reportConfiguration.setApprovedLicenseNames(approvedLicenses.stream().map(license -> {
            SimpleLicenseFamily simpleLicenseFamily = new SimpleLicenseFamily();
            simpleLicenseFamily.setFamilyName(license);
            return simpleLicenseFamily;
        }).toArray(SimpleLicenseFamily[]::new));

        ClaimStatistic stats = generateReport(reportConfiguration, getReportFile());
        boolean unknownLicenses = stats.getNumUnknown() > 0;
        boolean unApprovedLicenses = stats.getNumUnApproved() > 0;
        if (unknownLicenses || unApprovedLicenses) {
            getLogger().error("The following files contain unapproved license headers:");
            unapprovedFiles(getReportFile()).stream().forEachOrdered(unapprovedFile -> getLogger().error(unapprovedFile));
            throw new GradleException("Check failed. License header problems were found. Full details: " + reportFile.getAbsolutePath());
        }
    }

    private IHeaderMatcher subStringMatcher(String licenseFamilyCategory, String licenseFamilyName, String substringPattern) {
        SubstringLicenseMatcher substringLicenseMatcher = new SubstringLicenseMatcher();
        substringLicenseMatcher.setLicenseFamilyCategory(licenseFamilyCategory);
        substringLicenseMatcher.setLicenseFamilyName(licenseFamilyName);

        SubstringLicenseMatcher.Pattern pattern = new SubstringLicenseMatcher.Pattern();
        pattern.setSubstring(substringPattern);
        substringLicenseMatcher.addConfiguredPattern(pattern);
        return substringLicenseMatcher;
    }

    private ClaimStatistic generateReport(ReportConfiguration config, File xmlReportFile) {
        try {
            Files.deleteIfExists(reportFile.toPath());
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(xmlReportFile));
            return toXmlReportFile(config, bufferedWriter);
        } catch (IOException | RatException exception) {
            throw new GradleException("Cannot generate license header report for " + getPath(), exception);
        }
    }

    private ClaimStatistic toXmlReportFile(ReportConfiguration config, Writer writer) throws RatException, IOException {
        ClaimStatistic stats = new ClaimStatistic();
        RatReport standardReport = XmlReportFactory.createStandardReport(new XmlWriter(writer), stats, config);

        standardReport.startReport();
        for (FileCollection dirSet : getSourceFolders().get()) {
            for (File f : dirSet.getAsFileTree().matching(patternFilterable -> patternFilterable.exclude(getExcludes()))) {
                standardReport.report(new FileDocument(f));
            }
        }
        standardReport.endReport();
        writer.flush();
        writer.close();
        return stats;
    }

    private static List<String> unapprovedFiles(File xmlReportFile) {
        try {
            NodeList resourcesNodes = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(xmlReportFile)
                .getElementsByTagName("resource");
            return elementList(resourcesNodes).stream()
                .filter(
                    resource -> elementList(resource.getChildNodes()).stream()
                        .anyMatch(n -> n.getTagName().equals("license-approval") && n.getAttribute("name").equals("false"))
                )
                .map(e -> e.getAttribute("name"))
                .sorted()
                .collect(Collectors.toList());
        } catch (SAXException | IOException | ParserConfigurationException e) {
            throw new GradleException("Error parsing xml report " + xmlReportFile.getAbsolutePath());
        }
    }

    private static List<Element> elementList(NodeList resourcesNodes) {
        List<Element> nodeList = new ArrayList<>(resourcesNodes.getLength());
        for (int idx = 0; idx < resourcesNodes.getLength(); idx++) {
            nodeList.add((Element) resourcesNodes.item(idx));
        }
        return nodeList;
    }
}
