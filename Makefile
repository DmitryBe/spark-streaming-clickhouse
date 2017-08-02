build:
	sbt compile
	sbt pack

pack:
	sbt pack-archive
