#!/bin/sh
#
# build zip package, but ensuring its from the current core/ source
# turn off tests and other validation to speed it up
# TODO: can be sped up more, if shading is moved out of core/
# TODO: this will work on windows too. feel free to make a .bat
mvn -pl core,distribution/zip package -DskipTests -Drun -Pdev
