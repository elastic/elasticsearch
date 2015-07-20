<?xml version="1.0" encoding="UTF-8"?>
<!-- remove this when junit4 summary format is fixed -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
  <xsl:template match="/">
    <failsafe-summary>
      <xsl:attribute name="timeout">
        <xsl:value-of select="failsafe-summary/@timeout"/>
      </xsl:attribute>
      <completed><xsl:value-of select="failsafe-summary/@completed"/></completed>
      <errors><xsl:value-of select="failsafe-summary/@errors"/></errors>
      <failures><xsl:value-of select="failsafe-summary/@failures"/></failures>
      <skipped><xsl:value-of select="failsafe-summary/@skipped"/></skipped>
      <failureMessage><xsl:value-of select="failsafe-summary/@failureMessage"/></failureMessage>
    </failsafe-summary>
  </xsl:template>
</xsl:stylesheet>
