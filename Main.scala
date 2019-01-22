package jobqueue

import collection.mutable.PriorityQueue
import scala.util.parsing.json._

object Main extends App {

	// Instantiate job queue object
	val jobQueue = new JobQueue(Map[JobType, PriorityQueue[Job]](), Map[String, Agent]())

	// Read file lines from STDIN and parse them to scala data structures
	val fileLines: Option[List[Map[String, Map[String, Any]]]] = {
		var lines = ""
		var line = ""
		while ({line = scala.io.StdIn.readLine(); line != null}) {
		    lines += line + "\n"
		}
		JSON.parseFull(lines).asInstanceOf[Option[List[Map[String, Map[String, Any]]]]]
	}

	// Keep assignments for agents and jobs
	var jobAgentAssignments = List[Map[String, Map[String, String]]]()

	// Walk through input data and execute commands
	fileLines.foreach(_.foreach(_.foreach{ case (command, content) =>
		command match {

			// Handle add new agent
			case "new_agent" =>
				
				def parseSkills(s: Any): Set[JobType] = {
					s match {
						case l: List[_] => l.map(e => JobType(e.toString)).toSet
						case _ => Set()
					}
				}

				// Add new agent to job queue.
				jobQueue.newAgent(
					Agent(
						content("id").toString,
						parseSkills(content("primary_skillset")),
						parseSkills(content("secondary_skillset"))
					)
				)

			// Handle add new job
			case "new_job" =>

				jobQueue.newJob(
					Job(
						content("id").toString,
						JobType(content("type").toString),
						content("urgent").asInstanceOf[Boolean]
					)
				)

			// Handle dequeue job request
			case "job_request" =>
				val agentId = content("agent_id").toString
				val job = jobQueue.dequeue(agentId)

				jobAgentAssignments ++= List(Map("job_assigned" -> Map("job_id" -> job.map(_.id).getOrElse("null"), "agent_id" -> agentId)))
				
			case _ => println("Invalid command")
		}

	}))

	// Convert assignments to json
	val jobAgentAssignmentsJSON = JSONArray(jobAgentAssignments.flatMap(_.map { case (k, v) => JSONObject(Map(k -> JSONObject(v)))}))
	// Print to STDOUT
	println(jobAgentAssignmentsJSON)
}