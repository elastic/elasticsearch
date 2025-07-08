/// *
// * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// * or more contributor license agreements. Licensed under the "Elastic License
// * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
// * Public License v 1"; you may not use this file except in compliance with, at
// * your election, the "Elastic License 2.0", the "GNU Affero General Public
// * License v3.0 only", or the "Server Side Public License, v 1".
// */
//
// package org.elasticsearch;
//
// import org.elasticsearch.common.VersionId;
// import org.elasticsearch.xcontent.ConstructingObjectParser;
// import org.elasticsearch.xcontent.ParseField;
// import org.elasticsearch.xcontent.XContentParser;
// import org.elasticsearch.xcontent.XContentParserConfiguration;
// import org.elasticsearch.xcontent.json.JsonXContent;
//
// import java.io.BufferedReader;
// import java.io.File;
// import java.io.IOException;
// import java.io.InputStream;
// import java.io.InputStreamReader;
// import java.net.URL;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
// import java.util.Map;
// import java.util.function.Function;
// import java.util.stream.Collectors;
//
// import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
//
/// **
// * Represents the version of the wire protocol used to communicate between a pair of ES nodes.
// * <p>
// * Prior to 8.8.0, the release {@link Version} was used everywhere. This class separates the wire protocol version from the release
/// version.
// * <p>
// * Each transport version constant has an id number, which for versions prior to 8.9.0 is the same as the release version for backwards
// * compatibility. In 8.9.0 this is changed to an incrementing number, disconnected from the release version.
// * <p>
// * Each version constant has a unique id string. This is not actually used in the binary protocol, but is there to ensure each protocol
// * version is only added to the source file once. This string needs to be unique (normally a UUID, but can be any other unique nonempty
// * string). If two concurrent PRs add the same transport version, the different unique ids cause a git conflict, ensuring that the second
/// PR
// * to be merged must be updated with the next free version first. Without the unique id string, git will happily merge the two versions
// * together, resulting in the same transport version being used across multiple commits, causing problems when you try to upgrade between
// * those two merged commits.
// *
// * <h2>Version compatibility</h2>
// * The earliest compatible version is hardcoded in the {@link TransportVersions#MINIMUM_COMPATIBLE} field. Previously, this was
/// dynamically
// * calculated from the major/minor versions of {@link Version}, but {@code TransportVersion} does not have separate major/minor version
// * numbers. So the minimum compatible version is hard-coded as the transport version used by the highest minor release of the previous
// * major version. {@link TransportVersions#MINIMUM_COMPATIBLE} should be updated appropriately whenever a major release happens.
// * <p>
// * The earliest CCS compatible version is hardcoded at {@link TransportVersions#MINIMUM_CCS_VERSION}, as the transport version used by the
// * previous minor release. This should be updated appropriately whenever a minor release happens.
// *
// * <h2>Scope of usefulness of {@link TransportVersion2}</h2>
// * {@link TransportVersion2} is a property of the transport connection between a pair of nodes, and should not be used as an indication of
// * the version of any single node. The {@link TransportVersion2} of a connection is negotiated between the nodes via some logic that is
/// not
// * totally trivial, and may change in future. Any other places that might make decisions based on this version effectively have to
/// reproduce
// * this negotiation logic, which would be fragile. If you need to make decisions based on the version of a single node, do so using a
// * different version value. If you need to know whether the cluster as a whole speaks a new enough {@link TransportVersion2} to understand
/// a
// * newly-added feature, use {@link org.elasticsearch.cluster.ClusterState#getMinTransportVersion}.
// */
// public class TransportVersion2 implements VersionId<TransportVersion2> {
// private String name;
// private final int[] futureTVs;
// private final int localTV;
// private final int[] backportTVs; // TODO use int[] here instead?
//
//
// // TODO: There will be two sources - locally defined ones, and others coming over the wire. Make sure we account for the wire case.
// private TransportVersion2(String name, int[] tvIds) {
// this.name = name; // TODO: does a null value here mean it came from the wire?
// assert tvIds != null;
// assert checkVersionIdsAreSorted(tvIds) : "Transport version ids must be sorted";
//
// // Find the localTV
// int localTvIndex = -1;
// for (int i = 0; i < tvIds.length; i++) {
// if (tvIds[i] <= current().id()) {
// localTvIndex = i;
// break;
// }
// }
//
// // TODO is this true?
// if (localTvIndex == -1) {
// throw new IllegalArgumentException("TransportVersionsGroup must contain a local TransportVersion");
// }
//
// this.localTV = tvIds[localTvIndex];
// this.futureTVs = Arrays.copyOfRange(tvIds, 0, localTvIndex);
// this.backportTVs = Arrays.copyOfRange(tvIds, localTvIndex + 1, tvIds.length);
// }
//
//
// private static boolean checkVersionIdsAreSorted(int[] tvIds) {
// for (int i = 1; i < tvIds.length; i++) {
// if (tvIds[i - 1] < (tvIds[i])) {
// return false;
// }
// }
// return true;
// }
//
// @Override
// public int id() {
// return localTV;
// }
//
// public int[] allIds() {
// var allIds = new int[futureTVs.length + 1 + backportTVs.length];
// System.arraycopy(futureTVs, 0, allIds, 0, futureTVs.length);
// allIds[futureTVs.length] = localTV; // TODO check for off by 1
// System.arraycopy(backportTVs, 0, allIds, futureTVs.length + 1, backportTVs.length);
// return allIds;
// }
//
// public String name() {
// return name;
// }
//
// public static TransportVersion2 declare(String name) {
// var tv = VersionsHolder.NAME_TO_VERSION_MAP.get(name);
// if (tv == null) {
// throw new IllegalStateException("transport version id [" + name + "] not found"); // TODO describe how to run the task.
// }
// return tv;
// }
//
//
// // public static TransportVersion2 readVersion(StreamInput in) throws IOException {
//// return fromId(in.readString());
//// }
//
// /**
// * Finds a {@code TransportVersion} by its id.
// * If a transport version with the specified ID does not exist,
// * this method creates and returns a new instance of {@code TransportVersion} with the specified ID.
// * The new instance is not registered in {@code TransportVersion.getAllVersions}.
// */
// public static TransportVersion2 fromId(int id) {
// TransportVersion2 known = VersionsHolder.LOCAL_ID_TO_VERSION_MAP.get(id);
// if (known != null) {
// return known;
// }
// // this is a version we don't otherwise know about - just create a placeholder
// return new TransportVersion2(null, new int[]{id});
// }
//
//// public static void writeVersion(TransportVersion2 version, StreamOutput out) throws IOException {
//// out.writeOptionalString(version.name()); // TODO optional because unknown version from wire might not have a name.
//// out.writeIntArray(version.); // TODO: vInt?
//// out.writeVInt(version.id);
//// }
//
// /**
// * Returns the minimum version of {@code version1} and {@code version2}
// */
// public static TransportVersion2 min(TransportVersion2 version1, TransportVersion2 version2) {
// return version1.id() < version2.id() ? version1 : version2;
// }
//
// /**
// * Returns the maximum version of {@code version1} and {@code version2}
// */
// public static TransportVersion2 max(TransportVersion2 version1, TransportVersion2 version2) {
// return version1.id() > version2.id() ? version1 : version2;
// }
//
// /**
// * Returns {@code true} if the specified version is compatible with this running version of Elasticsearch.
// */
//// public static boolean isCompatible(TransportVersion2 version) {
//// return version.onOrAfter(TransportVersions.MINIMUM_COMPATIBLE);
//// }
//
// /**
// * Reference to the most recent transport version.
// * This should be the transport version with the highest id.
// */
// public static TransportVersion2 current() {
// return VersionsHolder.CURRENT;
// }
//
// /**
// * Sorted list of all defined transport versions
// */
// public static List<TransportVersion2> getAllVersions() {
// return VersionsHolder.ALL_VERSIONS;
// }
//
// /**
// * @return whether this is a known {@link TransportVersion2}, i.e. one declared in {@link TransportVersions}. Other versions may exist
// * in the wild (they're sent over the wire by numeric ID) but we don't know how to communicate using such versions.
// */
// public boolean isKnown() {
// return VersionsHolder.LOCAL_ID_TO_VERSION_MAP.containsKey(id());
// }
//
// /**
// * @return the newest known {@link TransportVersion2} which is no older than this instance. Returns {@link TransportVersions#ZERO} if
// * there are no such versions.
// */
//// public TransportVersion2 bestKnownVersion() {
//// if (isKnown()) {
//// return this;
//// }
//// TransportVersion2 bestSoFar = TransportVersions.ZERO;
//// for (final var knownVersion : VersionsHolder.LOCAL_ID_TO_VERSION_MAP.values()) {
//// if (knownVersion.after(bestSoFar) && knownVersion.before(this)) {
//// bestSoFar = knownVersion;
//// }
//// }
//// return bestSoFar;
//// }
// public static TransportVersion2 fromString(String str) {
// return TransportVersion2.fromId(Integer.parseInt(str));
// }
//
// /**
// * Returns {@code true} if this version is a patch version at or after {@code version}.
// * <p>
// * This should not be used normally. It is used for matching patch versions of the same base version,
// * using the standard version number format specified in {@link TransportVersions}.
// * When a patch version of an existing transport version is created, {@code transportVersion.isPatchFrom(patchVersion)}
// * will match any transport version at or above {@code patchVersion} that is also of the same base version.
// * <p>
// * For example, {@code version.isPatchFrom(8_800_0_04)} will return the following for the given {@code version}:
// * <ul>
// * <li>{@code 8_799_0_00.isPatchFrom(8_800_0_04)}: {@code false}</li>
// * <li>{@code 8_799_0_09.isPatchFrom(8_800_0_04)}: {@code false}</li>
// * <li>{@code 8_800_0_00.isPatchFrom(8_800_0_04)}: {@code false}</li>
// * <li>{@code 8_800_0_03.isPatchFrom(8_800_0_04)}: {@code false}</li>
// * <li>{@code 8_800_0_04.isPatchFrom(8_800_0_04)}: {@code true}</li>
// * <li>{@code 8_800_0_49.isPatchFrom(8_800_0_04)}: {@code true}</li>
// * <li>{@code 8_800_1_00.isPatchFrom(8_800_0_04)}: {@code false}</li>
// * <li>{@code 8_801_0_00.isPatchFrom(8_800_0_04)}: {@code false}</li>
// * </ul>
// */
// public boolean isPatchFrom(TransportVersion2 version) {
// return onOrAfter(version) && id() < version.id() + 100 - (version.id() % 100);
// }
//
// /**
// * Returns a string representing the Elasticsearch release version of this transport version,
// * if applicable for this deployment, otherwise the raw version number.
// */
// public String toReleaseVersion() {
// return TransportVersions.VERSION_LOOKUP.apply(id());
// }
//
// @Override
// public String toString() {
// return Integer.toString(id());
// }
//
//
// private static class VersionsHolder {
// private static final List<TransportVersion2> ALL_VERSIONS;
// private static final List<TransportVersion2> LOCALLY_DEFINED_VERSIONS;
// private static final Map<Integer, TransportVersion2> LOCAL_ID_TO_VERSION_MAP;
// private static final Map<String, TransportVersion2> NAME_TO_VERSION_MAP;
// private static final TransportVersion2 CURRENT;
//
//
// static {
// try {
// var versionMetadata = loadVersionMetadata();
// LOCALLY_DEFINED_VERSIONS = versionMetadata.stream()
// .map(v -> new TransportVersion2(v.name(), v.ids())).toList();
// ALL_VERSIONS = LOCALLY_DEFINED_VERSIONS; // TODO add serverless to the mix
// } catch (IOException e) {
// throw new RuntimeException(e); // todo add a meaningfull exception
// }
//
//
//// Collection<TransportVersion2> extendedVersions = ExtensionLoader.loadSingleton(ServiceLoader.load(VersionExtension.class))
//// .map(VersionExtension::getTransportVersions)
//// .orElse(Collections.emptyList());
////
//// if (extendedVersions.isEmpty()) {
//// ALL_VERSIONS = DEFINED_VERSIONS;
//// } else {
//// ALL_VERSIONS = Stream.concat(DEFINED_VERSIONS.stream(), extendedVersions.stream()).sorted().toList();
//// }
//
// LOCAL_ID_TO_VERSION_MAP = ALL_VERSIONS.stream()
// .collect(Collectors.toUnmodifiableMap(TransportVersion2::id, Function.identity()));
// NAME_TO_VERSION_MAP = ALL_VERSIONS.stream()
// .collect(Collectors.toUnmodifiableMap(TransportVersion2::name, Function.identity()));
//
// CURRENT = ALL_VERSIONS.getLast();
// }
//
// public static List<VersionMetaData> loadVersionMetadata() throws IOException {
// List<VersionMetaData> metadataList = new ArrayList<>();
//
// // Get the class loader to access resources
// ClassLoader classLoader = VersionMetaData.class.getClassLoader();
// final String path = "org/elasticsearch/transport";
//
// // Find resource files using resource listing technique
// URL directoryURL = classLoader.getResource(path);
// if (directoryURL != null && "file".equals(directoryURL.getProtocol())) {
// File directory = new File(directoryURL.getFile());
// if (directory.exists()) {
// File[] files = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".json"));
// if (files != null) {
// for (File file : files) {
// try (InputStream is = classLoader.getResourceAsStream(path + "/" + file.getName())) {
// if (is != null) {
// // Parse the JSON file into VersionMetaData
// metadataList.add(parseVersionMetaData(is));
// }
// }
// }
// }
// }
// } else {
// // For JAR files, use resource listing helper
// try (InputStream is = classLoader.getResourceAsStream(path);
// BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
// String line;
// while ((line = reader.readLine()) != null) {
// if (line.toLowerCase().endsWith(".json")) {
// try (InputStream fileStream = classLoader.getResourceAsStream(path + "/" + line)) {
// if (fileStream != null) {
// metadataList.add(parseVersionMetaData(fileStream));
// }
// }
// }
// }
// }
// }
//
// return metadataList;
// }
//
// private static VersionMetaData parseVersionMetaData(InputStream inputStream) throws IOException {
// // Create parser from the input stream
// XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, inputStream);
//
// // Parse the VersionMetaData using the provided parser method
// return VersionMetaData.fromXContent(parser);
// }
//
// private record VersionMetaData(String name, int[] ids) {
// public static ConstructingObjectParser<VersionMetaData, Void> PARSER = new ConstructingObjectParser<>(
// "version_meta_data",
// false,
// args -> new VersionMetaData((String) args[0], (int[]) args[1])
// );
//
// static {
// PARSER.declareString(constructorArg(), new ParseField("name"));
// PARSER.declareIntArray(constructorArg(), new ParseField("ids"));
// }
//
// public static VersionMetaData fromXContent(XContentParser parser) throws IOException {
// return PARSER.apply(parser, null);
// }
// }
// }
// }
