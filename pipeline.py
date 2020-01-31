#!/usr/bin/env python3


from queue import Queue
from threading import Thread
import logging
import reviewshake
import my_db
import time


#  Configure the logger
#  See https://docs.python.org/3/howto/logging-cookbook.html
logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)-8s %(asctime)s %(name)-12s %(message)s',
                    datefmt='%H:%M:%S %m/%d/%Y',
                    filename='pipeline.log',
                    filemode='w')

# The logger is configured to write to the file. This adds a second handler
# which writes the logs to the console.
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

N_START_JOB_THREADS = 5
N_MONITOR_JOB_THREADS = 5
N_PROCESS_OUTPUT_THREADS = 5


def start_jobs(pending_job_queue, running_job_queue):
    """
    Runs a loop which takes a job from the pending job queue, starts it, then puts the running job
    into the running job queue to be handled by a monitor worker.
    """
    while True:
        job_to_start = pending_job_queue.get()
        if job_to_start is None:
            break
        logging.debug('Starting job %s', str(job_to_start))
        running_job = reviewshake.start_job(job_to_start)
        if running_job is not None:
            running_job_queue.put(running_job)
        pending_job_queue.task_done()


def monitor_running_jobs(running_job_queue, finished_job_queue):
    """
    Runs a loop which takes a running reviewshake job and checks if it's done being processed by
    reviewshake. If the job is done then it's placed into the finished_job_queue to have the results
    processed.
    """
    wait_time = 1.0  # seconds
    while True:
        job_to_monitor = running_job_queue.get()
        if job_to_monitor is None:
            break
        for _ in range(25):
            result = reviewshake.get_job_status(job_to_monitor['id'])
            if result['crawl_status'] == 'complete':
                logging.info("job %s completed crawling", job_to_monitor['id'])
                finished_job_queue.put((job_to_monitor, result))
                break
            else:
                logging.debug("job %s not complete, waiting...", job_to_monitor['id'])
            time.sleep(wait_time)
        else:
            logging.error("job %d did not not complete", job_to_monitor['id'])
        running_job_queue.task_done()


def process_finished_jobs(finished_job_queue):
    """
    Runs a loop which processes the reviews from a finished reviewshake job.
    """
    while True:
        item = finished_job_queue.get()
        if item is None:
            break
        (finished_job, result) = item
        logging.info("job %d completed with %d results", finished_job['id'], result['result_count'])
        reviews = reviewshake.get_reviews(finished_job['id'], result['result_count'])
        for review in reviews:
            pass  # TODO Do stuff with the review here.
        finished_job_queue.task_done()


def main():

    #  Initialize the queues
    pending_job_queue = Queue(maxsize=0)
    running_job_queue = Queue(maxsize=0)
    finished_job_queue = Queue(maxsize=0)

    #  Populate the initial queue with the jobs to be processed
    for customer in my_db.customers():
        for job in reviewshake.create_customer_jobs(customer):
            pending_job_queue.put(job)  # beginning of the pipeline

    #  Create and start the worker threads to start the reviewshake jobs
    for _ in range(N_START_JOB_THREADS):
        new_worker = Thread(target=start_jobs, args=(pending_job_queue, running_job_queue))
        new_worker.daemon = True
        new_worker.start()

    #  Create and start the worker threads to monitor the runnin reviewshake jobs
    for _ in range(N_MONITOR_JOB_THREADS):
        new_worker = Thread(target=monitor_running_jobs, args=(running_job_queue, finished_job_queue))
        new_worker.daemon = True
        new_worker.start()

    #  Create and start the worker threads to process the reviews from completed reviewshake jobs
    for _ in range(N_PROCESS_OUTPUT_THREADS):
        new_worker = Thread(target=process_finished_jobs, args=(finished_job_queue,))
        new_worker.daemon = True
        new_worker.start()

    # Wait for the queues to be empty before terminating the program
    pending_job_queue.join()
    logging.info("All jobs started")
    running_job_queue.join()
    logging.info("All jobs completed")
    finished_job_queue.join()
    logging.info("All results processed")


if __name__ == '__main__':
    main()
