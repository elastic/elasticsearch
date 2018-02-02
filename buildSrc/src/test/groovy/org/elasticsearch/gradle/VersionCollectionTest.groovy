package org.elasticsearch.gradle

class VersionCollectionTest extends GroovyTestCase {

  String formatVersion(String version) {
    return " public static final Version V_${version.replaceAll("\\.", "_")} "
  }
  def allVersions = [formatVersion('5.0.0'), formatVersion('5.0.0_alpha1'), formatVersion('5.0.0_alpha2'), formatVersion('5.0.0_beta1'),
                     formatVersion('5.0.0_rc1'),formatVersion('5.0.0_rc2'),formatVersion('5.0.1'), formatVersion('5.0.2'),
                     formatVersion('5.1.1'), formatVersion('5.1.2'),
                     formatVersion('5.2.0'), formatVersion('5.2.1'), formatVersion('5.2.2'), formatVersion('5.3.0'), formatVersion('5.3.1'),
                     formatVersion('5.3.2'), formatVersion('5.3.3'), formatVersion('5.4.0'),
                     formatVersion('5.4.1'), formatVersion('5.4.2'), formatVersion('5.4.3'), formatVersion('5.5.0'), formatVersion('5.5.1'),
                     formatVersion('5.5.2'), formatVersion('5.5.3'), formatVersion('5.6.0'), formatVersion('5.6.1'), formatVersion('5.6.2'),
                     formatVersion('5.6.3'), formatVersion('5.6.4'), formatVersion('5.6.5'), formatVersion('5.6.6'), formatVersion('5.6.7'),
                     formatVersion('5.6.8'), formatVersion('6.0.0'),
                     formatVersion('6.0.1'), formatVersion('6.1.0'), formatVersion('6.1.1'), formatVersion('6.1.2'), formatVersion('6.1.3'),
                     formatVersion('6.1.4'), formatVersion('6.2.0'), formatVersion('6.3.0'),
                     formatVersion('7.0.0_alpha1'), formatVersion('7.0.0_alpha2')]

  void testAgainstMajorUnreleasedWithExistingStagedMinorRelease() {
    VersionCollection vc = new VersionCollection(allVersions)
    assertNotNull(vc)
    assertEquals(vc.nextMinorSnapshot, Version.fromString("6.3.0-SNAPSHOT"))
    assertEquals(vc.stagedMinorSnapshot, Version.fromString("6.2.0-SNAPSHOT"))
    assertEquals(vc.nextBugfixSnapshot, Version.fromString("6.1.4-SNAPSHOT"))
    assertEquals(vc.maintenanceBugfixSnapshot, Version.fromString("5.6.8-SNAPSHOT"))
  }

  void testAgainstMinorReleasedBranch() {
    List localVersion = allVersions.clone()
    localVersion.removeAll { it.toString().contains('7_0_0')} // remove all the 7.x so that the actual version is 6.3 (6.x)
    VersionCollection vc = new VersionCollection(localVersion)
    assertNotNull(vc)
    assertEquals(vc.nextMinorSnapshot, null)
    assertEquals(vc.stagedMinorSnapshot, Version.fromString("6.2.0-SNAPSHOT"))
    assertEquals(vc.nextBugfixSnapshot, Version.fromString("6.1.4-SNAPSHOT"))
    assertEquals(vc.maintenanceBugfixSnapshot, Version.fromString("5.6.8-SNAPSHOT"))
  }
}
