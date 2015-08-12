::
:: build zip package, but ensuring its from the current source
:: turn off tests and other validation to speed it up
:: TODO: can be sped up more, if shading is moved out of core/
CALL mvn -am -pl dev-tools,distribution/zip package -DskipTests -Drun -Pdev
