name := "cats-effect-tutorial"

version := "3.3.14"

scalaVersion := "2.13.8"

libraryDependencies += "org.typelevel" %% "cats-effect" % "3.3.14" withSources() withJavadoc()

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:postfixOps"
)