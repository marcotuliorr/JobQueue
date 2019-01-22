package jobqueue

import collection.mutable.PriorityQueue

/** Define the types that jobs may have.
  * @param name: Name of the job type.
  */
case class JobType(name: String)

/**	Define a job and its attributes.
  * @param id: Job identifier.
  * @param jobType: Type of job to be executed.
  * @param urgent: true, if job is urgent; false, otherwise.
  *	@param order: Sequence order indicating when job was added to queue.
  */
case class Job(id: String, jobType: JobType, urgent: Boolean = false, order: Int = 0) extends Ordered[Job] {

	// Comparison method for prioritization of jobs
	def compare(that: Job): Int = {
		(this.urgent, that.urgent) match {
			case (true, false) => 1
			case (false, true) => -1
			case _ if (this.order < that.order) => 1
			case _ if (this.order > that.order) => -1
			case _ => 0
		}
	}

}

/** Defines an agent and its attributes.
  * @param id: Agent identifier.
  * @param primarySkills: Set of primary skills of the agent.
  * @param secondarySkills: Set of secondary skills of the agent.
  */
case class Agent(id: String, primarySkills: Set[JobType], secondarySkills: Set[JobType])


/** Class that defines the job queue.
  * Jobs are stored in a map structure that maps its job type to the jobs belonging to this
  * specific type (this improves access in method dequeue()). For each job type, there
  * exists a priority queue of jobs that prioritizes the most relevant job for that job type.
  * Agents are stored in a map, that links the agent id to the agent itself.
  * @param jobs: Map between JobType and PriorityQueue[Job], for jobs belonging to the JobType.
  * @param agents: Map between agent id and agent object.
*/
class JobQueue(
	private var jobs: Map[JobType, PriorityQueue[Job]],
	private var agents: Map[String, Agent]
) {

	// Arrival order of next added job.
	var order = 1

	// Adds new agent.
	// Complexity: constant
	def newAgent(agent: Agent): Unit = agents += (agent.id -> agent)

	// Adds new job to queue.
	// Complexity: O(log(n))
	def newJob(job: Job): Unit = {
		val jt: JobType = job.jobType
		jobs.get(jt) match {
			case Some(pq) => pq.enqueue(job.copy(order = order))
			case None => jobs = jobs + (jt -> PriorityQueue[Job](job.copy(order = order)))
		}
		// Increase next job order.
		order += 1
	}

	// Dequeues a job from jobs according to agent.
	// Complexity: O(log(n))
	def dequeue(agentId: String): Option[Job] = {

		val bestJob = agents.get(agentId).flatMap { agent =>

			/*  Get priority job according to given set of job types.
				Create a priority queue of priority jobs among all priority queues
				for given agent skills. Take the priority job amongst all.
			*/
			def getPriorityJobForAgentSkills(agentSkills: Set[JobType]) : Option[Job] = {
				val priorityJobsForSkills = agentSkills.toList.flatMap { sk =>
					jobs.get(sk).flatMap(_.headOption).toList
				}

				// Add priority jobs among agent skills to priority queue
				val pq = PriorityQueue[Job]()
				pq ++= priorityJobsForSkills

				// Take priority job among skills priority jobs
				pq.headOption
			}
			
			val priorityJobPrimarySkills = getPriorityJobForAgentSkills(agent.primarySkills)
			if (priorityJobPrimarySkills.isDefined) priorityJobPrimarySkills
			else getPriorityJobForAgentSkills(agent.secondarySkills)
		}

		// Remove job from queue
		bestJob.foreach(j => jobs(j.jobType).dequeue())

		bestJob
	}
}