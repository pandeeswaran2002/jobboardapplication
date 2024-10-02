from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr, validator
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson import ObjectId, errors as bson_errors
from typing import List, Optional
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client.job_board
jobs_collection = db.jobs
applications_collection = db.applications

# Pydantic Models
class ObjectIdModel(BaseModel):
    __root__: str

    class Config:
        arbitrary_types_allowed = True

class JobPosting(BaseModel):
    title: str
    description: str
    company_name: str
    location: str
    posted_date: datetime
    salary: Optional[float] = None  # Changed to float for numerical operations
    tags: Optional[List[str]] = None

    @validator('salary', pre=True, always=True)
    def validate_salary(cls, v):
        if v is not None:
            try:
                return float(v)
            except ValueError:
                raise ValueError("Salary must be a number.")
        return v

class JobApplication(BaseModel):
    job_id: ObjectIdModel
    applicant_name: str
    email: EmailStr
    resume: str
    application_date: datetime

app = FastAPI()

def job_to_dict(job):
    job_dict = job.copy()
    job_dict["id"] = str(job_dict["_id"])
    del job_dict["_id"]
    return job_dict

def application_to_dict(application):
    app_dict = application.copy()
    app_dict["id"] = str(app_dict["_id"])
    app_dict["job_id"] = str(app_dict["job_id"])
    del app_dict["_id"]
    return app_dict

# Ensure indexes are created
def create_indexes():
    # Text index for full-text search
    jobs_collection.create_index([
        ("title", "text"),
        ("description", "text"),
        ("company_name", "text")
    ], name="text_index")

    # Additional indexes for performance
    jobs_collection.create_index([("posted_date", ASCENDING)], name="posted_date_index")
    jobs_collection.create_index([("salary", ASCENDING)], name="salary_index")
    jobs_collection.create_index([("tags", ASCENDING)], name="tags_index")

create_indexes()

# CRUD Endpoints for Job Postings

@app.post("/jobs/create", response_model=JobPosting)
async def create_job(job: JobPosting):
    try:
        job_dict = job.dict()
        job_dict["_id"] = ObjectId()
        jobs_collection.insert_one(job_dict)
        logger.info(f"Job created with ID: {job_dict['_id']}")
        return job_to_dict(job_dict)
    except Exception as e:
        logger.error(f"Error creating job: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/jobs/get", response_model=List[JobPosting])
async def get_jobs(
    location: Optional[str] = Query(None, description="Filter jobs by location"),
    company_name: Optional[str] = Query(None, description="Filter jobs by company name"),
    tags: Optional[List[str]] = Query(None, description="Filter jobs by tags/skills as an array")
):
    try:
        query = {}
        if location:
            query["location"] = location
        if company_name:
            query["company_name"] = company_name
        if tags:
            query["tags"] = {"$all": tags}  # Ensure all specified tags are present

        jobs = [job_to_dict(job) for job in jobs_collection.find(query)]
        logger.info(f"Retrieved {len(jobs)} jobs with query: {query}")
        return jobs
    except Exception as e:
        logger.error(f"Error retrieving jobs: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/jobs/get/{id}", response_model=JobPosting)
async def get_job(id: str):
    try:
        job = jobs_collection.find_one({"_id": ObjectId(id)})
        if job:
            logger.info(f"Job found with ID: {id}")
            return job_to_dict(job)
        logger.warning(f"Job not found with ID: {id}")
        raise HTTPException(status_code=404, detail="Job not found")
    except bson_errors.InvalidId:
        logger.error(f"Invalid Job ID format: {id}")
        raise HTTPException(status_code=400, detail="Invalid Job ID format")
    except Exception as e:
        logger.error(f"Error retrieving job: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.put("/jobs/update/{id}", response_model=JobPosting)
async def update_job(id: str, job: JobPosting):
    try:
        job_dict = job.dict()
        # Ensure the salary is a float
        if job_dict.get("salary") is not None:
            job_dict["salary"] = float(job_dict["salary"])
        result = jobs_collection.update_one({"_id": ObjectId(id)}, {"$set": job_dict})
        if result.matched_count == 0:
            logger.warning(f"Job not found for update with ID: {id}")
            raise HTTPException(status_code=404, detail="Job not found")
        updated_job = jobs_collection.find_one({"_id": ObjectId(id)})
        logger.info(f"Job updated with ID: {id}")
        return job_to_dict(updated_job)
    except bson_errors.InvalidId:
        logger.error(f"Invalid Job ID format for update: {id}")
        raise HTTPException(status_code=400, detail="Invalid Job ID format")
    except ValueError as ve:
        logger.error(f"ValueError during job update: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Error updating job: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.delete("/jobs/delete/{id}")
async def delete_job(id: str):
    try:
        result = jobs_collection.delete_one({"_id": ObjectId(id)})
        if result.deleted_count == 0:
            logger.warning(f"Job not found for deletion with ID: {id}")
            raise HTTPException(status_code=404, detail="Job not found")
        logger.info(f"Job deleted with ID: {id}")
        return {"detail": "Job deleted"}
    except bson_errors.InvalidId:
        logger.error(f"Invalid Job ID format for deletion: {id}")
        raise HTTPException(status_code=400, detail="Invalid Job ID format")
    except Exception as e:
        logger.error(f"Error deleting job: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# CRUD Endpoints for Job Applications

@app.post("/application/create/", response_model=JobApplication)
async def create_application(application: JobApplication):
    try:
        application_dict = application.dict()
        application_dict["job_id"] = ObjectId(application.job_id.__root__)  # Convert to ObjectId
        application_dict["_id"] = ObjectId()
        applications_collection.insert_one(application_dict)
        logger.info(f"Application created with ID: {application_dict['_id']}")
        return application_to_dict(application_dict)
    except bson_errors.InvalidId:
        logger.error(f"Invalid Job ID format in application: {application.job_id.__root__}")
        raise HTTPException(status_code=400, detail="Invalid Job ID format")
    except Exception as e:
        logger.error(f"Error creating application: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/application/get/{id}", response_model=JobApplication)
async def get_application(id: str):
    try:
        application = applications_collection.find_one({"_id": ObjectId(id)})
        if application:
            logger.info(f"Application found with ID: {id}")
            return application_to_dict(application)
        logger.warning(f"Application not found with ID: {id}")
        raise HTTPException(status_code=404, detail="Application not found")
    except bson_errors.InvalidId:
        logger.error(f"Invalid Application ID format: {id}")
        raise HTTPException(status_code=400, detail="Invalid Application ID format")
    except Exception as e:
        logger.error(f"Error retrieving application: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/jobs/{job_id}/applications")
async def get_applications_for_job(job_id: str):
    try:
        # Validate job_id
        ObjectId(job_id)  # Will raise an exception if invalid

        query = {"job_id": ObjectId(job_id)}
        application_count = applications_collection.count_documents(query)
        applications = [application_to_dict(app) for app in applications_collection.find(query)]

        logger.info(f"Found {application_count} applications for Job ID: {job_id}")

        return {
            "job_id": job_id,
            "application_count": application_count,
            "applications": applications
        }
    except bson_errors.InvalidId:
        logger.error(f"Invalid Job ID format for applications retrieval: {job_id}")
        raise HTTPException(status_code=400, detail="Invalid Job ID format")
    except Exception as e:
        logger.error(f"Error retrieving applications for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# Advanced Search Endpoint

@app.get("/jobs/search", response_model=List[JobPosting])
async def search_jobs(
    q: Optional[str] = Query(None, description="Full-text search query"),
    min_salary: Optional[float] = Query(None, description="Minimum salary filter"),
    max_salary: Optional[float] = Query(None, description="Maximum salary filter"),
    posted_after: Optional[datetime] = Query(None, description="Filter jobs posted after a specific date"),
    skills: Optional[List[str]] = Query(None, description="Filter jobs by required skills (tags)"),
    sort_by: Optional[str] = Query("posted_date", description="Sort by 'posted_date', 'salary', or 'relevance'"),
    order: Optional[str] = Query("asc", description="Order of sorting: 'asc' or 'desc'"),
    page: Optional[int] = Query(1, ge=1, description="Page number for pagination"),
    limit: Optional[int] = Query(10, ge=1, le=100, description="Number of results per page")
):
    try:
        query = {}
        
        # Full-text search
        if q:
            query["$text"] = {"$search": q}
        
        # Salary range filtering
        if min_salary is not None or max_salary is not None:
            salary_query = {}
            if min_salary is not None:
                salary_query["$gte"] = min_salary
            if max_salary is not None:
                salary_query["$lte"] = max_salary
            query["salary"] = salary_query
        
        # Posted date filtering
        if posted_after:
            query["posted_date"] = {"$gte": posted_after}
        
        # Skills filtering
        if skills:
            query["tags"] = {"$all": skills}
        
        # Determine sort order
        if order.lower() == "asc":
            sort_order = ASCENDING
        elif order.lower() == "desc":
            sort_order = DESCENDING
        else:
            logger.error(f"Invalid sort order: {order}")
            raise HTTPException(status_code=400, detail="Invalid sort order. Must be 'asc' or 'desc'.")
        
        # Determine sort field
        if sort_by not in ["posted_date", "salary", "relevance"]:
            logger.error(f"Invalid sort_by field: {sort_by}")
            raise HTTPException(status_code=400, detail="Invalid sort_by field. Must be 'posted_date', 'salary', or 'relevance'.")
        
        # Handle sorting by relevance
        if sort_by == "relevance" and q:
            projection = {"score": {"$meta": "textScore"}}
            jobs_cursor = jobs_collection.find(query, projection).sort([("score", {"$meta": "textScore"})])
        else:
            sort_field = sort_by
            jobs_cursor = jobs_collection.find(query).sort(sort_field, sort_order)
        
        # Pagination
        skip = (page - 1) * limit
        jobs_cursor = jobs_cursor.skip(skip).limit(limit)
        
        # Execute the query and convert results
        jobs = [job_to_dict(job) for job in jobs_cursor]
        logger.info(f"Search executed with query: {query}, sort_by: {sort_by}, order: {order}, page: {page}, limit: {limit}")
        
        return jobs

    except ValueError as ve:
        logger.error(f"ValueError during search: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Unexpected error during job search: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

