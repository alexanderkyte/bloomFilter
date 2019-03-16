lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.kyte.alex",
      scalaVersion := "2.12.7"
    )),
    name := "bloom-filter"
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
