
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr, validator, Field
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson import ObjectId, errors as bson_errors
from typing import List, Optional,Dict, Union, Any
from datetime import datetime
import logging
from pymongo.errors import PyMongoError





# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client.job_board
jobs_collection = db.jobs
applications_collection = db.applications
job_views_collection = db.job_views
status_collection = db.application.status



# Pydantic Models
class ObjectIdModel(BaseModel):
    __root__: str

    class Config:
        arbitrary_types_allowed = True
class app_status(BaseModel):
    job_id: ObjectIdModel
    total: int
    reviewing: int
    interview_scheduled: int
    offered: int 
    rejected: int
    


class JobPosting(BaseModel):
    title: str
    description: str
    company_name: str
    location: str
    posted_date: datetime
    salary: Optional[float] = None 
    tags: Optional[List[str]] = None

class BulkUpdateRequest(BaseModel):
    job_ids: List[str] = Field(..., description="List of job IDs to update.")
    update: Dict[str, Any] = Field(..., description="Fields to update with their new values.")





class JobRecommendation(BaseModel):
    user_id: str
    recommended_jobs: List[Dict[str, Any]]



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
    applicationstatus: str


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
def app_status_dict(status):
    status_dict = status.copy()
    status_dict["id"] = str(status_dict["_id"])
    status_dict["job_id"] = str(status_dict["job_id"])
    del status_dict["_id"]
    return status_dict

def create_indexes():    
    jobs_collection.create_index([
        ("title", "text"),
        ("description", "text"),
        ("company_name", "text")
    ], name="text_index")

   
    jobs_collection.create_index([("posted_date", ASCENDING)], name="posted_date_index")
    jobs_collection.create_index([("salary", ASCENDING)], name="salary_index")
    jobs_collection.create_index([("tags", ASCENDING)], name="tags_index")

create_indexes()



@app.post("/jobs/createtheposting/", response_model=JobPosting)
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







@app.get("/jobs/gettheposting/", response_model=List[JobPosting])
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
            query["tags"] = {"$all": tags}  

        jobs = [job_to_dict(job) for job in jobs_collection.find(query)]
        logger.info(f"Retrieved {len(jobs)} jobs with query: {query}")
        return jobs
    except Exception as e:
        logger.error(f"Error retrieving jobs: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/jobs/get/{id}basedretrive/", response_model=JobPosting)
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

@app.put("/jobs/update/{id}basedupdate", response_model=JobPosting)
async def update_job(id: str, job: JobPosting):
    try:
        job_dict = job.dict()
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

@app.delete("/jobs/delete/{id}deletethejobpost")
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


@app.post("/application/createtheapp/", response_model=JobApplication)
async def create_application(application: JobApplication):
    try:
        application_dict = application.dict()
        application_dict["job_id"] = ObjectId(application.job_id.__root__)  
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

@app.get("/application/get/{id}retrivedata", response_model=JobApplication)
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

@app.get("/jobs/{job_id}/applicationscount")
async def get_applications_for_job(job_id: str):
    try:

        ObjectId(job_id)  

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



@app.get("/jobs/searchbasedonmutipelfield/", response_model=List[JobPosting])
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
        
     
        if q:
            query["$text"] = {"$search": q}
        

        if min_salary is not None or max_salary is not None:
            salary_query = {}
            if min_salary is not None:
                salary_query["$gte"] = min_salary
            if max_salary is not None:
                salary_query["$lte"] = max_salary
            query["salary"] = salary_query
        
       
        if posted_after:
            query["posted_date"] = {"$gte": posted_after}
        
        if skills:
            query["tags"] = {"$all": skills}
        
     
        if order.lower() == "asc":
            sort_order = ASCENDING
        elif order.lower() == "desc":
            sort_order = DESCENDING
        else:
            logger.error(f"Invalid sort order: {order}")
            raise HTTPException(status_code=400, detail="Invalid sort order. Must be 'asc' or 'desc'.")
        
  
        if sort_by not in ["posted_date", "salary", "relevance"]:
            logger.error(f"Invalid sort_by field: {sort_by}")
            raise HTTPException(status_code=400, detail="Invalid sort_by field. Must be 'posted_date', 'salary', or 'relevance'.")
        
    
    
        if sort_by == "relevance" and q:
            projection = {"score": {"$meta": "textScore"}}
            jobs_cursor = jobs_collection.find(query, projection).sort([("score", {"$meta": "textScore"})])
        else:
            sort_field = sort_by
            jobs_cursor = jobs_collection.find(query).sort(sort_field, sort_order)
        
      
        skip = (page - 1) * limit
        jobs_cursor = jobs_cursor.skip(skip).limit(limit)
        

        jobs = [job_to_dict(job) for job in jobs_cursor]
        logger.info(f"Search executed with query: {query}, sort_by: {sort_by}, order: {order}, page: {page}, limit: {limit}")
        
        return jobs

    except ValueError as ve:
        logger.error(f"ValueError during search: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Unexpected error during job search: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

for job in jobs_collection.find({"salary": {"$type": "string"}}):
    try:
        new_salary = float(job['salary'])
        jobs_collection.update_one(
            {"_id": job['_id']},
            {"$set": {"salary": new_salary}}
        )
        print(f"Updated salary for job ID: {job['_id']}")
    except ValueError:
        print(f"Invalid salary format for job ID: {job['_id']}")
@app.get("/jobs/jobpostoveralldetails/")
async def get_job_stats():
    try:
        pipeline = [
            {
                "$group": {
                    "_id": "$location",
                    "total_jobs": { "$sum": 1 },
                    "average_salary": { "$avg": "$salary" },
                    "min_salary": { "$min": "$salary" },
                    "max_salary": { "$max": "$salary" }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_jobs": { "$sum": "$total_jobs" },
                    "average_salary": { "$avg": "$average_salary" },
                    "min_salary": { "$min": "$min_salary" },
                    "max_salary": { "$max": "$max_salary" },
                    "jobs_per_location": {
                        "$push": { "location": "$_id", "count": "$total_jobs" }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "total_jobs": 1,
                    "average_salary": { "$round": ["$average_salary", 0] },
                    "min_salary": 1,
                    "max_salary": 1,
                    "jobs_per_location": {
                        "$arrayToObject": {
                            "$map": {
                                "input": "$jobs_per_location",
                                "as": "loc",
                                "in": { "k": "$$loc.location", "v": "$$loc.count" }
                            }
                        }
                    }
                }
            }
        ]
        
        stats = list(jobs_collection.aggregate(pipeline))[0]
        return stats
    
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error")
@app.post("/jobs/{id}/view")
async def track_job_view(id: str):
    job_id = ObjectId(id)
    job_views_collection.insert_one({"job_id": job_id, "viewed_at": datetime.utcnow()})

@app.get("/jobs/{id}/analyticstheviewcountofjobpost/")
async def get_job_analytics(id: str):
    job_id = ObjectId(id)

  
    total_views = job_views_collection.count_documents({"job_id": job_id})

    views_per_day = list(job_views_collection.aggregate([
        {"$match": {"job_id": job_id}},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$viewed_at"}},
            "views": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]))

  
    views_per_week = list(job_views_collection.aggregate([
        {"$match": {"job_id": job_id}},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%U", "date": "$viewed_at"}},
            "views": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]))


    views_per_month = list(job_views_collection.aggregate([
        {"$match": {"job_id": job_id}},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m", "date": "$viewed_at"}},
            "views": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]))


    return {
        "job_id": id,
        "total_views": total_views,
        "views_per_day": {view["_id"]: view["views"] for view in views_per_day},
        "views_per_week": {view["_id"]: view["views"] for view in views_per_week},
        "views_per_month": {view["_id"]: view["views"] for view in views_per_month}
    }
@app.post("/jobs/bulkrquestatonce/")
async def create_bulk_jobs(jobs: List[JobPosting]):

    job_dicts = [job.dict(exclude_unset=True) for job in jobs]

    result = jobs_collection.insert_many(job_dicts)

    return {"inserted_ids": [str(id) for id in result.inserted_ids]}
@app.put("/jobs/bulkupdates/", response_model=Dict[str, int])
async def bulk_update_jobs(request: BulkUpdateRequest):
    try:
        
        filter_criteria = {"_id": {"$in": [ObjectId(job_id) for job_id in request.job_ids]}}
        update_doc = {"$set": request.update}

    
        result = jobs_collection.update_many(filter_criteria, update_doc)

        logger.info(f"Updated {result.modified_count} jobs successfully.")
        return {"updated_count": result.modified_count}

    except bson_errors.InvalidId as e:
        logger.error(f"Invalid Job ID format: {e}")
        raise HTTPException(status_code=400, detail="Invalid Job ID format")
    except Exception as e:
        logger.error(f"Error updating jobs: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
@app.get("/job-recommendationsforuser/{user_id}", response_model=JobRecommendation)
async def get_job_recommendations(user_id: str):
    try:
        
        applied_jobs = applications_collection.find({"email": user_id})
        applied_job_ids = [str(job["job_id"]) for job in applied_jobs]


        viewed_jobs = job_views_collection.find({"user_id": user_id})
        viewed_job_ids = [str(job["job_id"]) for job in viewed_jobs]

       
        all_interacted_job_ids = set(applied_job_ids + viewed_job_ids)

       
        recommended_jobs = []
        for job in jobs_collection.find({"_id": {"$nin": [ObjectId(job_id) for job_id in all_interacted_job_ids]}}):
            recommended_jobs.append(job_to_dict(job))

        logger.info(f"Recommended {len(recommended_jobs)} jobs for user ID: {user_id}")
        return JobRecommendation(user_id=user_id, recommended_jobs=recommended_jobs)
    except Exception as e:
        logger.error(f"Error retrieving job recommendations: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
@app.post("/jobstatus/createthestatus/", response_model=app_status)
async def create_application(status: app_status):
    try:
        status_dict = status.dict()
        status_dict["job_id"] = ObjectId(status.job_id.__root__)  
        status_dict["_id"] = ObjectId()
        status_collection.insert_one(status_dict)
        logger.info(f"status created with ID: {status_dict['_id']}")
        return app_status_dict(status_dict)
    except bson_errors.InvalidId:
        logger.error(f"Invalid Job ID format in statusapp: {status.job_id.__root__}")
        raise HTTPException(status_code=400, detail="Invalid Job ID format")
    except Exception as e:
        logger.error(f"Error creating application: {e}")
        raise HTTPException(status_code=500, detail="Internal Server")
@app.get("/jobs/{id}/applications/statusretrive/", response_model=app_status)
async def get_appstatus(id: str):
    try:
        status = status_collection.find_one({"_id": ObjectId(id)})
        if status:
            logger.info(f"status found with ID: {id}")
            return application_to_dict(status)
        logger.warning(f"status not found with ID: {id}")
        raise HTTPException(status_code=404, detail="status not found")
    except bson_errors.InvalidId:
        logger.error(f"Invalid Application ID format: {id}")
        raise HTTPException(status_code=400, detail="Invalid status ID format")
    except Exception as e:
        logger.error(f"Error retrieving application: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.put("/applications/{application_id}/statusupdate/")
async def update_application_status(application_id: str, status: str):
    try:
  
        application_obj_id = ObjectId(application_id)

        result = applications_collection.update_one(
            {"_id": application_obj_id},
            {"$set": {"applicationstatus": status}}
        )

        if result.matched_count == 0:
            logger.warning(f"Application not found with ID: {application_id}")
            raise HTTPException(status_code=404, detail="Application not found")

        logger.info(f"Application status updated to '{status}' for ID: {application_id}")
        return {"detail": f"Application status updated to '{status}'."}

    except bson_errors.InvalidId:
        logger.error(f"Invalid Application ID format: {application_id}")
        raise HTTPException(status_code=400, detail="Invalid Application ID format")
    except Exception as e:
        logger.error(f"Error updating application status: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

