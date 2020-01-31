import logging
import os
import random


headers = {
    'spiderman-token': os.environ.get('REVIEWSHAKE_API_KEY', 'FAKE_TEST_KEY')
}


def create_customer_jobs(customer):
    """
    Logic to create reviewshake jobs, essentially the add_jobs function in dailyDataLoadV4d.py

    This is a mock function
    """
    for platform in ['Yelp', 'VRBO', 'TripAdvisor']:
        customer_job_url = "not-sure-what-this-is.com"
        new_job = {
            'customer': customer,
            'platform': platform,
            'url': customer_job_url
        }
        logging.info("created a new customer job: %s", str(new_job))
        yield new_job


def start_job(job):
    """
    This is where you'd do the POST request to reviewshake to add the job, then add the job id to the job

    This is a mock function
    """
    if random.random() < 0.01:  # 1% chance of simulated failure
        logging.error("Failed to start job: %s", str(job))
        return None
    job['id'] = random.randint(10000, 99999)
    return job


def get_job_status(job_id):
    """
    get_job_status from dailyDataLoadV4d.py

    This is a mock function
    """
    if random.random() < 0.25:
        return {
            "crawl_status": "complete",
            "result_count": random.randint(3, 50)
        }
    else:
        return {"crawl_status": "incomplete"}


def get_reviews(job_id, result_count):
    """
    get_reviews from dailyDataLoadV4d.py

    I don't know how to mock the reviews so this is basically the end of the simulation.
    """
    for review_number in range(result_count):
        yield "review number {} for job id {}".format(review_number, job_id)
